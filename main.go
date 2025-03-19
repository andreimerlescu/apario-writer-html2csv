package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fatih/color"

	check "github.com/andreimerlescu/checkfs"
	"github.com/andreimerlescu/checkfs/file"
	"github.com/andreimerlescu/configurable"
	sema "github.com/andreimerlescu/go-sema"
	"golang.org/x/net/html"
)

const prefixUrl string = "https://archives.gov"

// Record represents a downloadable file's metadata
type Record struct {
	URL      string
	Path     string
	Filename string
}

// DownloadResult captures the outcome of a download attempt
type DownloadResult struct {
	URL string
	Err error
}

var sem sema.Semaphore
var cfg configurable.IConfigurable

const (
	kInput     string = "input"
	kOutput    string = "output"
	kPDFs      string = "pdfs"
	kDownloads string = "downloads"
	kErrorLog  string = "error_log"
)

func init() {
	cfg = configurable.New()
	cfg.NewString(kInput, "./input.html", "Path to input HTML file to parse")
	cfg.NewString(kOutput, "./output.csv", "Path to the output CSV file")
	cfg.NewString(kPDFs, "./pdfs/", "Path to downloaded PDFs")
	cfg.NewInt(kDownloads, 9, "Concurrent downloads to allow")
	cfg.NewString(kErrorLog, "./error.log", "Path to error log file")
}

func main() {
	// Parse configuration
	cfgFile := os.Getenv("CONFIG_FILE")
	if len(cfgFile) > 0 {
		if err := check.File(cfgFile, file.Options{Exists: true}); err == nil {
			if err := cfg.Parse(cfgFile); err != nil {
				log.Fatalf("Error parsing config file %s: %v", cfgFile, err)
			}
		}
	} else if err := cfg.Parse(""); err != nil {
		log.Fatalf("Error parsing default config: %v", err)
	}

	// Set up error logger
	errorLogPath := *cfg.String(kErrorLog)
	logFile, err := os.OpenFile(errorLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open error log file %s: %v", errorLogPath, err)
	}
	defer logFile.Close()
	logger := log.New(logFile, "", log.LstdFlags)

	// Validate downloads concurrency
	if *cfg.Int(kDownloads) <= 0 {
		logger.Fatal("Concurrent downloads must be positive")
	}
	sem = sema.New(*cfg.Int(kDownloads))

	// Check input file
	if *cfg.String(kInput) == "" {
		logger.Fatal("Input HTML file path is required. Use -input flag.")
	}

	// Read HTML file
	htmlContent, err := os.ReadFile(*cfg.String(kInput))
	if err != nil {
		logger.Fatalf("Failed to read HTML file %s: %v", *cfg.String(kInput), err)
	}

	// Parse HTML
	records, err := parseHTML(string(htmlContent))
	if err != nil {
		logger.Fatalf("Failed to parse HTML: %v", err)
	}

	// Create PDFs directory
	pdfDir := *cfg.String(kPDFs)
	if err := os.MkdirAll(pdfDir, 0755); err != nil {
		logger.Fatalf("Failed to create PDFs directory %s: %v", pdfDir, err)
	}

	// Set up context and channels
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errChan := make(chan DownloadResult, len(records))
	doneChan := make(chan struct{})
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Schedule all downloads
	wg := sync.WaitGroup{}
	for _, record := range records {
		wg.Add(1)
		go func(r Record) {
			defer wg.Done()
			err := r.downloadURL(ctx)
			errChan <- DownloadResult{URL: r.URL, Err: err}
		}(record)
	}

	// Wait for all downloads to complete
	go func() {
		wg.Wait()
		close(errChan)
		doneChan <- struct{}{}
	}()

	// Main event loop
	for {
		select {
		case <-ctx.Done():
			logger.Println("Context canceled, exiting...")
			os.Exit(1)
		case <-doneChan:
			// Process all download results
			failedURLs := []string{}
			for result := range errChan {
				if result.Err != nil {
					failedURLs = append(failedURLs, result.URL)
					logger.Printf("Error downloading %s: %v", result.URL, result.Err)
				}
			}
			if len(failedURLs) > 0 {
				color.Red("Failed to download %d URLs:", len(failedURLs))
				for _, url := range failedURLs {
					color.Red("- %s", url)
				}
			} else {
				color.Green("All downloads completed successfully.")
			}
			if err := writeCSV(*cfg.String(kOutput), records); err != nil {
				logger.Fatalf("Failed to write CSV: %v", err)
			}
			fmt.Printf("Successfully wrote %d records to %s\n", len(records), *cfg.String(kOutput))
			return
		case sig := <-sigChan:
			logger.Printf("Received signal %v, shutting down...", sig)
			cancel()
			os.Exit(1)
		}
	}
}

// downloadURL handles the download of a single URL with progress reporting
func (r *Record) downloadURL(ctx context.Context) (err error) {
	color.Blue("Starting download of %s", r.URL)
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		if err != nil {
			color.Red("Failed to download %s in %v: %v", r.URL, duration, err)
		} else {
			color.Green("Downloaded %s in %v", r.URL, duration)
		}
	}()

	sem.Acquire()
	defer sem.Release()

	recordFilePath := filepath.Join(*cfg.String(kPDFs), r.Filename)

	if err = check.File(recordFilePath, file.Options{Exists: true}); err == nil {
		color.Yellow("Skipping %s: file already exists", recordFilePath)
		return nil
	}

	fullURL := strings.Clone(r.URL)
	if !strings.HasPrefix(fullURL, `http`) {
		fullURL = prefixUrl + r.URL
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP status %d", resp.StatusCode)
	}

	out, err := os.Create(recordFilePath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

// parseHTML extracts download records from HTML content
func parseHTML(htmlContent string) ([]Record, error) {
	doc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return nil, err
	}

	var records []Record
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "tr" {
			var url, filename string
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && c.Data == "td" {
					if c.FirstChild != nil && c.FirstChild.Type == html.ElementNode && c.FirstChild.Data == "a" {
						for _, attr := range c.FirstChild.Attr {
							if attr.Key == "href" {
								url = attr.Val
								filename = filepath.Base(url)
								break
							}
						}
					}
				}
			}
			if url != "" {
				path := filepath.Join(*cfg.String(kPDFs), filename)
				records = append(records, Record{
					URL:      url,
					Path:     path,
					Filename: filename,
				})
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)

	return records, nil
}

// writeCSV writes the records to a CSV file
func writeCSV(outputPath string, records []Record) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write([]string{"URL", "PATH", "FILENAME"})
	if err != nil {
		return err
	}

	for _, record := range records {
		err = writer.Write([]string{record.URL, record.Path, record.Filename})
		if err != nil {
			return err
		}
	}

	return nil
}
