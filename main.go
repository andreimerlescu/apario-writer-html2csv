package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	check "github.com/andreimerlescu/checkfs"
	"github.com/andreimerlescu/checkfs/file"
	"github.com/andreimerlescu/figtree/v2"
	sema "github.com/andreimerlescu/go-sema"
	"github.com/fatih/color"
	"github.com/pdfcpu/pdfcpu/pkg/api"
	"golang.org/x/net/html"
)

const prefixUrl string = "https://archives.gov" // this data is fixed at archives.gov

// Record represents a downloadable file's metadata
type Record struct {
	URL      string
	Path     string
	Filename string
}

// DownloadResult captures the outcome of a download attempt
type DownloadResult struct {
	Err    error
	Record Record
}

var sem sema.Semaphore // limit system runtime resources to prevent overload or DDOS
var figs figtree.Fruit // fun cli configuration utility with validators and callbacks

const (
	kImport    string = "import"    // path to import csv file ; used as -import -input -output
	kInput     string = "input"     // path to input HTML file to parse
	kOutput    string = "output"    // path to output csv file
	kPDFs      string = "pdfs"      // path to downloaded PDFs
	kDownloads string = "downloads" // concurrent downloads allowed
	kErrorLog  string = "error_log" // path to error log file
)

// Row represents a single Record in the CSV Table
type Row struct {
	ID       string `json:"ID" yaml:"ID"`
	URL      string `json:"URL" yaml:"URL"`
	PATH     string `json:"PATH" yaml:"PATH"`
	FILENAME string `json:"FILENAME" yaml:"FILENAME"`
	PAGES    string `json:"PAGES" yaml:"PAGES"`
}

func (row *Row) populatePages(ctx context.Context) error {
	handler, err := os.Open(row.PATH)
	if err != nil {
		return err
	}

	info, err := api.PDFInfo(handler, row.PATH, nil, nil)
	if err != nil {
		return err
	}
	row.PAGES = strconv.FormatInt(int64(info.PageCount), 10)
	return nil
}

// TableHeaders define the -import headers that apario-writer requires for its CSV import
var TableHeaders = []string{"ID", "URL", "PATH", "FILENAME", "PAGES"}

// Table represents the output CSV data
type Table struct {
	Headers []string
	Rows    []Row
}

func init() {
	figs = figtree.Grow()
	figs.NewString(kInput, "./input.html", "Path to input HTML file to parse")
	figs.WithValidator(kInput, figtree.AssureStringNotEmpty)
	figs.NewString(kOutput, "./output.csv", "Path to the output CSV file")
	figs.WithValidator(kOutput, figtree.AssureStringNotEmpty)
	figs.NewString(kImport, "./import.csv", "Path to the import CSV file to append results in new output.csv")
	figs.WithValidator(kImport, figtree.AssureStringNotEmpty)
	figs.NewString(kPDFs, "./pdfs/", "Path to downloaded PDFs")
	figs.WithValidator(kPDFs, figtree.AssureStringNotEmpty)
	figs.NewInt(kDownloads, 9, "Concurrent downloads to allow")
	figs.WithValidator(kDownloads, figtree.AssureIntInRange(3, 17))
	figs.NewString(kErrorLog, "./error.log", "Path to error log file")
	figs.WithValidator(kErrorLog, figtree.AssureStringNotEmpty)
	figs.WithCallback(kErrorLog, figtree.CallbackAfterVerify, func(value interface{}) error {
		path, ok := value.(string)
		if !ok {
			_path, ok := value.(*string)
			if !ok {
				return fmt.Errorf("value is not a string")
			}
			path = *_path
		}
		if err := check.File(path, file.Options{Exists: true, Create: file.Create{
			Kind:     file.IfNotExists,
			OpenFlag: os.O_CREATE | os.O_TRUNC | os.O_RDWR,
			FileMode: 0644,
		}}); err != nil {
			return err
		}
		return nil
	})
	if err := figs.Load(); err != nil {
		log.Fatalf("error loading config: %v", err)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Set up error logger
	errorLogPath := *figs.String(kErrorLog)
	logFile, err := os.OpenFile(errorLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open error log file %s: %v", errorLogPath, err)
	}
	defer logFile.Close()
	logger := log.New(logFile, "", log.LstdFlags)

	// Validate downloads concurrency
	if *figs.Int(kDownloads) <= 0 {
		logger.Fatal("Concurrent downloads must be positive")
	}
	sem = sema.New(*figs.Int(kDownloads))

	// Check input file
	if *figs.String(kInput) == "" {
		logger.Fatal("Input HTML file path is required. Use -input flag.")
	}

	finalTable := Table{}             // where we gather the Record DownloadResults into
	finalTable.Headers = TableHeaders // capture the headers needed in the new output
	ftMutex := sync.RWMutex{}
	lastID := atomic.Int64{}
	yah := ""
	if len(*figs.String(kImport)) > 0 {
		importBytes, err := os.ReadFile(*figs.String(kImport)) // get the bytes from the file
		if err != nil {
			logger.Fatal(err)
		}
		records, err := csv.NewReader(bytes.NewReader(importBytes)).ReadAll() // read the records in the file
		if err != nil {
			logger.Fatal(err)
		}
		for _, record := range records { // iterate over the lines
			if l := len(record); l != 5 {
				logger.Printf("Skipping over row due to columns != 5; got = %d", l)
				continue
			}
			row := Row{}                                            // create a new row
			row.ID = record[0]                                      // first column is the ID
			thisID := new(int64)                                    // prepare a new ID placeholder
			items, err := fmt.Sscanf(row.ID, "%s-%d", &yah, thisID) // scan the JFKFILES-0001 integer out
			if err != nil {
				logger.Println(err)
			}
			if items == 1 && *thisID > lastID.Load() { // if we captured thisID and its greater than lastID
				color.Green("Got new lastID = %d ➜ %d", lastID, *thisID)
				lastID.Store(*thisID) // update the new value
			}
			row.URL = record[1]                         // url may have prexif on it or not
			if !strings.HasPrefix(row.URL, prefixUrl) { // if it doesnt
				row.URL = prefixUrl + row.URL // add it
			}
			row.PATH = record[2]
			if !strings.HasPrefix(row.PATH, *figs.String(kPDFs)) {
				color.Red("PATH record indicates mismatching directory for -pdfs due to:\n\n"+
					"row.PATH = %s\n"+
					"-pdfs = %s", row.PATH, *figs.String(kPDFs))
				logger.Fatal("cannot use -pdfs in this manner with -import")
			}
			row.FILENAME = record[3]
			checkPath := filepath.Base(row.PATH)
			if !strings.EqualFold(checkPath, row.FILENAME) {
				color.Red("FILENAME mismatch for -pdfs due to:\n\n"+
					"row.PATH = %s\n"+
					"row.FILENAME = %s\n"+
					"-pdfs = %s", row.PATH, row.FILENAME, *figs.String(kPDFs))
				logger.Fatal("cannot use -pdfs in this manner with -import")
			}
			row.PAGES = record[4]
			pages, _ := strconv.Atoi(row.PAGES)
			if pages == 0 {
				err := row.populatePages(ctx)
				if err != nil {
					logger.Println(err)
				}
			}
			ftMutex.Lock()
			finalTable.Rows = append(finalTable.Rows, row)
			ftMutex.Unlock()
		}
	}

	// Read HTML file
	htmlContent, err := os.ReadFile(*figs.String(kInput))
	if err != nil {
		logger.Fatalf("Failed to read HTML file %s: %v", *figs.String(kInput), err)
	}

	// Parse HTML
	records, err := parseHTML(string(htmlContent))
	if err != nil {
		logger.Fatalf("Failed to parse HTML: %v", err)
	}

	// Create PDFs directory
	pdfDir := *figs.String(kPDFs)
	if err := os.MkdirAll(pdfDir, 0755); err != nil {
		logger.Fatalf("Failed to create PDFs directory %s: %v", pdfDir, err)
	}

	// Set up context and channels
	errChan := make(chan DownloadResult, len(records))
	doneChan := make(chan struct{})
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)

	// Schedule all downloads
	wg := sync.WaitGroup{}
	for _, record := range records {
		wg.Add(1)
		go func(r Record) {
			defer wg.Done()
			err := r.downloadURL(ctx)
			errChan <- DownloadResult{Record: r, Err: err}
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
			var failedURLs []string
			wg := sync.WaitGroup{}
			for result := range errChan {
				if result.Err != nil {
					failedURLs = append(failedURLs, result.Record.URL)
					logger.Printf("Error downloading %s: %v", result.Record.URL, result.Err)
				}
				wg.Add(1)
				go func(wg *sync.WaitGroup, result DownloadResult) {
					defer wg.Done()
					row := Row{}
					thisID := lastID.Add(1)
					row.ID = fmt.Sprintf("%s-%4d", "JFKFILES", thisID)
					row.URL = result.Record.URL
					row.FILENAME = result.Record.Filename
					row.PATH = result.Record.Path
					err := row.populatePages(ctx)
					if err != nil {
						color.Red(err.Error())
						return
					}
					ftMutex.Lock()
					finalTable.Rows = append(finalTable.Rows, row)
					ftMutex.Unlock()

				}(&wg, result)
			}
			wg.Wait()
			if len(failedURLs) > 0 {
				color.Red("Failed to download %d URLs:", len(failedURLs))
				for _, url := range failedURLs {
					color.Red("- %s", url)
				}
			} else {
				color.Green("All downloads completed successfully.")
			}
			ftMutex.Lock()
			if err := writeCSV(*figs.String(kOutput), finalTable.Rows); err != nil {
				ftMutex.Unlock()
				logger.Fatalf("Failed to write CSV: %v", err)
			}
			ftMutex.Unlock()
			fmt.Printf("Successfully wrote %d records to %s\n", len(finalTable.Rows), *figs.String(kOutput))
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

	recordFilePath := filepath.Join(*figs.String(kPDFs), r.Filename)

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
				path := filepath.Join(*figs.String(kPDFs), filename)
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
func writeCSV(outputPath string, rows []Row) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write(TableHeaders)
	if err != nil {
		return err
	}

	for _, row := range rows {
		err = writer.Write([]string{row.ID, row.URL, row.PATH, row.FILENAME, row.PAGES})
		if err != nil {
			return err
		}
	}

	return nil
}
