package main

import (
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/gif"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/disintegration/imaging"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/nedpals/supabase-go"
	"github.com/redis/go-redis/v9"
	storage_go "github.com/supabase-community/storage-go"
)

type Status struct {
	Active bool `json:"active"`
	Upload int  `json:"upload"`
	Scale  int  `json:"scale"`
	Delete int  `json:"delete"`
}

var status = Status{
	Active: false,
	Scale:  20,
	Upload: 20,
	Delete: 10,
}

func HomeHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "Image converter server")
}

func IdToPath(id string, format string) string {
	return "./tmp/" + id + "." + format
}

func ResizeImage(path string) error {
	file, err := os.Open(path)
	if err != nil {
		log.Println("Resize image", "Open file", err)
		return err
	}

	defer file.Close()

	var img image.Image

	splitted := strings.Split(path, ".")
	if splitted[len(splitted)-1] == "gif" {

		gifImage, err := gif.Decode(file)
		if err != nil {
			log.Println("Resize image", "Image decode gif error", err)
			return err
		}
		img = gifImage
	} else {
		normalImage, _, err := image.Decode(file)
		if err != nil {
			log.Println("Resize image", "Image decode error", err)
			return err
		}

		img = normalImage
	}

	size := 300
	if strings.Contains(path, "information") {
		size = 500
	}

	resizedImage := imaging.Fit(img, size, size, imaging.CatmullRom)
	imaging.Save(resizedImage, path)
	return nil
}

func DownloadImage(dbImage DatabaseImage) (string, string, error) {
	splitted := strings.Split(dbImage.Url, "/")
	primary := strings.Join(splitted[:len(splitted)-1], "/")
	var fetchUrl = ""
	if strings.Contains(splitted[len(splitted)-1], ".") {
		dotSplitted := strings.Split(splitted[len(splitted)-1], ".")

		encodedLast := url.QueryEscape(strings.Join(dotSplitted[:len(dotSplitted)-1], ".")) + "." + strings.Join(dotSplitted[len(dotSplitted)-1:], "")
		fetchUrl = primary + "/" + encodedLast
	} else {
		fetchUrl = primary + "/" + splitted[len(splitted)-1]
	}

	response, err := http.Get(fetchUrl)

	if err != nil {
		log.Println("Download Image", "Get image", err, dbImage, fetchUrl)
		return "", "", err
	}

	defer response.Body.Close()

	contentType := response.Header.Get("Content-Type")
	format := strings.Split(contentType, "/")[1]

	dir := IdToPath(dbImage.Name, format)

	file, fileerr := os.Create(dir)
	if fileerr != nil {
		log.Println("Download image", "Create directory", fileerr, dir)
		return "", "", fileerr
	}

	defer file.Close()

	_, b := io.Copy(file, response.Body)

	if b != nil {
		log.Println("Download image", "Copy file to response", b)
		return "", "", b
	}

	return dir, contentType, nil
}

func ProcessImage(dbImage DatabaseImage, done chan LocalImage) error {
	path, contentType, err := DownloadImage(dbImage)
	if err != nil {
		return err
	}

	errResize := ResizeImage(path)
	if errResize != nil {
		return errResize
	}

	split := strings.Split(dbImage.Name, "-")
	store := split[0]
	storeId := split[1]
	imgType := split[2]
	id := strings.Join(split[3:], "-")

	done <- LocalImage{
		localPath:    path,
		externalPath: "/" + store + "/" + storeId + "/" + imgType + "/" + id,
		Id:           dbImage.Id,
		format:       contentType,
	}
	return nil
}

func GetImagesFromDatabase(count int, supabase *supabase.Client, ch chan DatabaseImage) int {
	var images []DatabaseImage
	dbErr := supabase.DB.From("cache_pending_images").Select("*").Limit(count).Eq("status", STATUS_PENDING).Execute(&images)

	if dbErr != nil {
		log.Println("Image from database", dbErr, count)
		return -1
	}

	var ids []string
	for i := 0; i < len(images); i++ {
		ids = append(ids, strconv.Itoa(images[i].Id))
	}

	var t []any
	dbUpdateErr := supabase.DB.From("cache_pending_images").Update(map[string]interface{}{"status": STATUS_CACHING}).In("id", ids).Execute(&t)

	if dbUpdateErr != nil {
		log.Println("Image from database", "Update caching status", dbUpdateErr, ids)
		return -1
	}

	for i := 0; i < len(images); i++ {
		ch <- images[i]
	}

	return len(images)
}

func UploadImage(dbImage LocalImage, storageClient *storage_go.Client) error {
	file, err := os.Open(dbImage.localPath)
	if err != nil {
		log.Println("Upload image", "Open file", err, dbImage)
		return err
	}

	stat, e := file.Stat()
	if e != nil {
		log.Println("Upload image", "Stats error", stat, e)
		return e
	}

	c, d := storageClient.UploadFile("images", dbImage.externalPath, file, storage_go.FileOptions{
		ContentType: &dbImage.format,
	})

	if d != nil {
		log.Println("Upload image", "Supabase upload", d, c)
		return d
	}

	file.Close()
	a := os.Remove(dbImage.localPath)

	if a != nil {
		log.Println("Upload image", "Remove file", a, dbImage)
		return a
	}

	return nil
}

func ProcessUploadImage(id int, input <-chan LocalImage, output chan LocalImage, supabase *storage_go.Client) {
	for {
		v, ok := <-input
		if !ok {
			return
		}

		e := UploadImage(v, supabase)
		if e != nil {
			log.Println("Error uploading image", v)
		}
		output <- v
	}
}

func HandleLocalImage(id int, input <-chan DatabaseImage, output chan LocalImage) {
	for {
		val, ok := <-input
		if !ok {
			return
		}

		err := ProcessImage(val, output)
		if err != nil {
			log.Println("Error process image", val)
		}
	}
}

func MultiDelete(images []LocalImage, supabase *supabase.Client) {
	var ids []string
	for i := 0; i < len(images); i++ {
		ids = append(ids, strconv.Itoa(images[i].Id))
	}

	var rows []any
	err := supabase.DB.From("cache_pending_images").Delete().In("id", ids).Execute(&rows)
	if err != nil {
		log.Println("Multi delete", "Delete caching", err, ids)
	}
}

func ProcessMultiDeleteRow(id int, input <-chan LocalImage, supabase *supabase.Client) {
	var images []LocalImage

	interval := 15 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case v, ok := <-input:
			if !ok {
				return
			}

			images = append(images, v)

			if len(images) > 10 {
				MultiDelete(images, supabase)
				images = nil
			}

		case <-ticker.C:
			if len(images) == 0 {
				continue
			}

			MultiDelete(images, supabase)
			images = nil
		}
	}
}

type DatabaseImage struct {
	Name   string `json:"name"`
	Url    string `json:"url"`
	Status string `json:"status"`
	Id     int    `json:"id"`
}

type LocalImage struct {
	localPath    string
	externalPath string
	Id           int `json:"id"`
	format       string
}

const STATUS_PENDING = "pending"
const STATUS_CACHING = "caching"

var processChannel = make(chan DatabaseImage, 100)
var doneChannel = make(chan LocalImage, 100)
var deleteChannel = make(chan LocalImage, 20)

type StatusMessage struct {
	Process []int `json:"process"`
	Done    []int `json:"done"`
	Delete  []int `json:"delete"`
}

func StatusHandler(w http.ResponseWriter, r *http.Request) {
	amountStr := r.URL.Query().Get("amount")
	amount, err := strconv.Atoi(amountStr)
	if err != nil {
		io.WriteString(w, "No amount defined as query")
		return
	}

	var wg sync.WaitGroup

	status := StatusMessage{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			if i < amount {
				status.Done = append(status.Done, len(doneChannel))
				status.Delete = append(status.Delete, len(deleteChannel))
				status.Process = append(status.Process, len(processChannel))
			} else {
				break
			}
			i += 1
		}
	}()

	wg.Wait()
	json, err := json.Marshal(status)
	if err != nil {
		io.WriteString(w, "Couldnt format status")
		return
	}

	w.Header().Set("content-type", "application/json")
	w.Write(json)
}

func ImageHandler(w http.ResponseWriter, r *http.Request, supabase *supabase.Client, redis *redis.Client, ctx *context.Context) {
	vars := mux.Vars(r)

	store := vars["store"]
	storeId := vars["storeId"]
	imageType := vars["type"]
	id := vars["id"]

	identifier := fmt.Sprintf("%s-%s-%s-%s", store, storeId, imageType, id)
	if redis != nil {
		redisLink, err := redis.Get(*ctx, identifier).Result()
		if err != nil && !strings.Contains(err.Error(), "redis: nil") {
			log.Println("Image handler", "Redis get error", err, redisLink)
		} else if redisLink != "" {
			http.Redirect(w, r, redisLink, http.StatusFound)
			return
		}
	}

	var rows []DatabaseImage
	supabaseErr := supabase.DB.From("cache_pending_images").Select("*").Eq("name", identifier).Execute(&rows)
	if supabaseErr != nil {
		log.Println("Image handler", "supabase caching error", supabaseErr, identifier)
		http.Redirect(w, r, "https://google.com", http.StatusNotFound)
		return
	}

	if len(rows) == 1 {
		http.Redirect(w, r, rows[0].Url, http.StatusFound)
		return
	}

	link := fmt.Sprintf("/v2/file/%s/%s/%s/%s", store, storeId, imageType, id)
	if redis != nil {
		redis.Set(*ctx, identifier, link, 10*time.Hour)
	}
	http.Redirect(w, r, link, http.StatusFound)
}

func ImageFileHandler(w http.ResponseWriter, r *http.Request, supabase *supabase.Client) {
	vars := mux.Vars(r)

	store := vars["store"]
	storeId := vars["storeId"]
	imageType := vars["type"]
	id := vars["id"]

	path := fmt.Sprintf("%s/%s/%s/%s", store, storeId, imageType, id)

	supabaseLink := supabase.Storage.From("images").GetPublicUrl(path).SignedUrl

	client := &http.Client{}
	req, err := http.NewRequest("GET", supabaseLink, nil)
	if err != nil {
		log.Println("Image file handler", "Error creating request to supabase", err, supabaseLink)
		http.Error(w, "Server error", http.StatusInternalServerError)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Println("Image file handler", "Error fetching file from supabase", err, req)
		http.Error(w, "Server error", http.StatusInternalServerError)
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Println("Image file handler", "File not found in supabase", resp.Status)
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}

	w.Header().Set("content-type", resp.Header.Get("content-type"))
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Println("Image file handler", "Error copying supabase file to response", err)
		http.Error(w, "Error serving file", http.StatusInternalServerError)
		return
	}
}

func main() {
	godotenv.Load()

	makeDirErr := os.MkdirAll("./tmp", os.ModePerm)
	if makeDirErr != nil {
		log.Println("Main", "Make tmp dir", makeDirErr)
		return
	}

	SUPABASE_URL := os.Getenv("SUPABASE_DEVELOPMENT_URL")
	SUPABASE_ANON_KEY := os.Getenv("SUPABASE_DEVELOPMENT_ANON_KEY")
	REDIS_URL := os.Getenv("REDIS_URL")

	supabase := supabase.CreateClient(SUPABASE_URL, SUPABASE_ANON_KEY)

	var storageClients []*storage_go.Client

	for i := 0; i < status.Upload; i++ {
		supabaseStorage := storage_go.NewClient(SUPABASE_URL+"/storage/v1", SUPABASE_ANON_KEY, nil)
		storageClients = append(storageClients, supabaseStorage)
	}

	opt, redisErr := redis.ParseURL(REDIS_URL)

	var redisClient *redis.Client

	if redisErr != nil {
		log.Println("Main", "No redis url provided", redisErr)
	} else {
		redisClient = redis.NewClient(opt)
	}
	ctx := context.Background()

	for i := 0; i < status.Scale; i++ {
		go HandleLocalImage(i, processChannel, doneChannel)
	}

	for i := 0; i < status.Upload; i++ {
		go ProcessUploadImage(i, doneChannel, deleteChannel, storageClients[i])
	}

	for i := 0; i < status.Delete; i++ {
		go ProcessMultiDeleteRow(i, deleteChannel, supabase)
	}

	go func() {
		for {
			if len(processChannel) < 50 {
				fetched := GetImagesFromDatabase(10, supabase, processChannel)
				if fetched == 0 {
					time.Sleep(time.Second)
				}
			}
		}
	}()

	fmt.Println("Starting GO Server")

	r := mux.NewRouter()
	r.HandleFunc("/", HomeHandler)
	r.HandleFunc("/status", StatusHandler)
	r.HandleFunc("/v2/store/{store}/image/{storeId}/{type}/{id}", func(res http.ResponseWriter, req *http.Request) {
		ImageHandler(res, req, supabase, redisClient, &ctx)
	})
	r.HandleFunc("/v2/file/{store}/{storeId}/{type}/{id}", func(res http.ResponseWriter, req *http.Request) {
		ImageFileHandler(res, req, supabase)
	})

	port := os.Getenv("PORT")
	url := "0.0.0.0:" + port

	server := &http.Server{
		Addr:         url,
		Handler:      r,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	fmt.Println("Server started on url: http://" + url)
	err := server.ListenAndServe()

	if err != nil {
		log.Fatal("Main", "Start server", err)
	}
}
