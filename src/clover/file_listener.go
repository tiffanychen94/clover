package main

import (
	"fmt"
	"github.com/go-fsnotify/fsnotify"
	"regexp"
	"strings"
	"time"
)

type FileListener interface {
	Run()
}

type fileListener struct {
	parser  parser
	watcher *fsnotify.Watcher
}

func NewFileListener(fileParser parser) (*fileListener, error) {
	newWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("Error initializing fsnotify file watcher: %s", err.Error())
	}

	return &fileListener{
		parser:  fileParser,
		watcher: newWatcher,
	}, nil
}

func (f *fileListener) Run() {
	fmt.Println("entering Run")
	done := make(chan bool)

	go func() {
		for {
			select {
			// watch for events
			case event := <-f.watcher.Events:
				fmt.Printf("Incoming Event: %#v\n", event)
				if event.Op == fsnotify.Create && strings.Contains(event.Name, "specs") {
					r, _ := regexp.Compile("specs/(.+).csv$")
					matches := r.FindStringSubmatch(event.Name)
					fmt.Println(matches[1])
					if matches != nil && len(matches) > 1 {
						// wait for write events to finish for the same file before firing
						time.Sleep(3000 * time.Millisecond)
						f.parser.ParseSpec(event.Name, matches[1])
					}

				} else if event.Op == fsnotify.Create && strings.Contains(event.Name, "data") {
					r, _ := regexp.Compile("data/(.+)_(.+).txt$")
					matches := r.FindStringSubmatch(event.Name)
					fmt.Println(matches[1])
					if matches != nil && len(matches) > 1 {
						// wait for write events to finish for the same file before firing
						time.Sleep(5000 * time.Millisecond)
						f.parser.StoreData(event.Name, matches[1])
					}
				}

			// watch for errors
			case err := <-f.watcher.Errors:
				fmt.Println("Error with file watcher:", err)
			}
		}
	}()

	if err := f.watcher.Add("specs"); err != nil {
		fmt.Println("Error adding specs directory", err)
	}

	if err := f.watcher.Add("data"); err != nil {
		fmt.Println("Error adding data directory", err)
	}

	<-done
}
