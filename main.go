package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/html"
)

// Config holds crawler configuration.
type Config struct {
	MaxDepth      int
	Concurrency   int
	Timeout       time.Duration
	UserAgent     string
	RespectRobots bool
}

// Result represents a crawled page.
type Result struct {
	URL              string   `json:"url"`
	Depth            int      `json:"depth"`
	Links            []string `json:"links,omitempty"`
	Error            string   `json:"error,omitempty"`
	RobotsDisallowed bool     `json:"robots_disallowed,omitempty"`
}

// Crawler implements the crawling logic.
type Crawler struct {
	config      Config
	visited     map[string]struct{}
	visitedMu   sync.RWMutex
	robotsMu    sync.RWMutex
	robotsRules map[string][]string // host -> disallowed prefixes
	httpClient  *http.Client
	wg          sync.WaitGroup
	jobCh       chan *workItem
	pending     uint64
	results     []Result
	resultsMu   sync.Mutex
}

type workItem struct {
	url   string
	depth int
}

// NewCrawler creates a new crawler with given config.
func NewCrawler(config Config) *Crawler {
	return &Crawler{
		config:      config,
		visited:     make(map[string]struct{}),
		robotsRules: make(map[string][]string),
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
		jobCh: make(chan *workItem, 1000), // buffered
	}
}

// addVisited marks a URL as visited.
func (c *Crawler) addVisited(url string) {
	c.visitedMu.Lock()
	defer c.visitedMu.Unlock()
	c.visited[url] = struct{}{}
}

// isVisited checks if a URL has been visited.
func (c *Crawler) isVisited(url string) bool {
	c.visitedMu.RLock()
	defer c.visitedMu.RUnlock()
	_, ok := c.visited[url]
	return ok
}

// addResult appends a result to the results slice.
func (c *Crawler) addResult(res Result) {
	c.resultsMu.Lock()
	defer c.resultsMu.Unlock()
	c.results = append(c.results, res)
}

// fetchRobots fetches and parses robots.txt for a given host.
func (c *Crawler) fetchRobots(host string) {
	c.robotsMu.Lock()
	defer c.robotsMu.Unlock()
	if _, exists := c.robotsRules[host]; exists {
		return // already fetched
	}
	robotsURL := fmt.Sprintf("http://%s/robots.txt", host)
	resp, err := c.httpClient.Get(robotsURL)
	if err != nil {
		// If cannot fetch robots.txt, assume no restrictions.
		c.robotsRules[host] = []string{}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// No robots.txt or error -> no restrictions.
		c.robotsRules[host] = []string{}
		return
	}
	var disallowed []string
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.robotsRules[host] = []string{}
		return
	}
	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Disallow:") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				path := strings.TrimSpace(parts[1])
				if path != "" {
					disallowed = append(disallowed, path)
				}
			}
		}
	}
	c.robotsRules[host] = disallowed
}

// isAllowed checks if the URL path is allowed by robots.txt for its host.
func (c *Crawler) isAllowed(rawURL string) bool {
	if !c.config.RespectRobots {
		return true
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := u.Hostname()
	c.robotsMu.RLock()
	rules, exists := c.robotsRules[host]
	c.robotsMu.RUnlock()
	if !exists {
		// Fetch robots.txt for this host.
		c.fetchRobots(host)
		c.robotsMu.RLock()
		rules, _ = c.robotsRules[host]
		c.robotsMu.RUnlock()
	}
	path := u.Path
	if path == "" {
		path = "/"
	}
	for _, prefix := range rules {
		if strings.HasPrefix(path, prefix) {
			return false
		}
	}
	return true
}

// extractLinks extracts all href links from HTML body.
func extractLinks(body []byte, baseURL *url.URL) []string {
	var links []string
	doc, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return links
	}
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, a := range n.Attr {
				if a.Key == "href" {
					link := a.Val
					if link == "" {
						continue
					}
					// Resolve relative URL.
					abs, err := baseURL.Parse(link)
					if err != nil {
						continue
					}
					links = append(links, abs.String())
					break
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)
	return links
}

// worker processes jobs from the job channel.
func (c *Crawler) worker() {
	defer c.wg.Done()
	for job := range c.jobCh {
		if c.isVisited(job.url) {
			atomic.AddUint64(&c.pending, ^uint64(0)) // decrement pending
			continue
		}
		if !c.isAllowed(job.url) {
			c.addResult(Result{
				URL:              job.url,
				Depth:            job.depth,
				RobotsDisallowed: true,
			})
			atomic.AddUint64(&c.pending, ^uint64(0)) // decrement pending
			continue
		}
		c.addVisited(job.url)
		resp, err := c.httpClient.Get(job.url)
		if err != nil {
			c.addResult(Result{
				URL:   job.url,
				Depth: job.depth,
				Error: err.Error(),
			})
			atomic.AddUint64(&c.pending, ^uint64(0)) // decrement pending
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			c.addResult(Result{
				URL:   job.url,
				Depth: job.depth,
				Error: fmt.Sprintf("HTTP %d", resp.StatusCode),
			})
			atomic.AddUint64(&c.pending, ^uint64(0)) // decrement pending
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			c.addResult(Result{
				URL:   job.url,
				Depth: job.depth,
				Error: err.Error(),
			})
			atomic.AddUint64(&c.pending, ^uint64(0)) // decrement pending
			continue
		}
		baseURL, _ := url.Parse(job.url)
		links := extractLinks(body, baseURL)
		c.addResult(Result{
			URL:   job.url,
			Depth: job.depth,
			Links: links,
		})
		if job.depth < c.config.MaxDepth {
			for _, link := range links {
				c.jobCh <- &workItem{url: link, depth: job.depth + 1}
				atomic.AddUint64(&c.pending, 1) // increment pending for new job
			}
		}
		atomic.AddUint64(&c.pending, ^uint64(0)) // decrement pending for this job
	}
}

// Start begins crawling from the seed URLs.
func (c *Crawler) Start(seeds []string) {
	// Start workers.
	for i := 0; i < c.config.Concurrency; i++ {
		c.wg.Add(1)
		go c.worker()
	}
	// Feed seeds.
	for _, seed := range seeds {
		atomic.AddUint64(&c.pending, 1) // increment pending for seed
		c.jobCh <- &workItem{url: seed, depth: 0}
	}
	// Wait for no pending work.
	for atomic.LoadUint64(&c.pending) > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	// Close job channel to signal workers to exit.
	close(c.jobCh)
	c.wg.Wait()
}

// Results returns the collected results as JSON string.
func (c *Crawler) Results() string {
	out, _ := json.MarshalIndent(c.results, "", "  ")
	return string(out)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <seed-url> [max-depth] [concurrency]\n", os.Args[0])
		os.Exit(1)
	}
	seed := os.Args[1]
	maxDepth := 2
	if len(os.Args) > 2 {
		fmt.Sscanf(os.Args[2], "%d", &maxDepth)
	}
	concurrency := 5
	if len(os.Args) > 3 {
		fmt.Sscanf(os.Args[3], "%d", &concurrency)
	}
	config := Config{
		MaxDepth:      maxDepth,
		Concurrency:   concurrency,
		Timeout:       15 * time.Second,
		UserAgent:     "WebCrawlerGo/1.0",
		RespectRobots: true,
	}
	crawler := NewCrawler(config)
	crawler.Start([]string{seed})
	fmt.Println(crawler.Results())
}
