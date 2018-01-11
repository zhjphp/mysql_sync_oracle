package main

import (
	"fmt"
	"log"
	"net/http"

	"m2o/controllers"

	"github.com/julienschmidt/httprouter"
)

func main() {
	router := httprouter.New()
	router.GET("/", Index)
	router.GET("/hello/:name", Hello)
	router.GET("/c/:act", controllersRun)
	router.POST("/c/:act", controllersRun)

	log.Fatal(http.ListenAndServe(":8888", router))
}

func controllersRun(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Transfer-Encoding", "chunked")
	var controller *controllers.ControllerType
	controller = new(controllers.ControllerType)
	controller.W = w
	controller.R = r
	controller.Ps = ps

	switch ps.ByName("act") {
	case "m2o":
		controller.M2oIndex()
		break
	default:
		fmt.Fprintf(w, "error, act not exist")
	}
}

func Index(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Fprint(w, "Welcome!\n")
}

func Hello(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Fprintf(w, "hello, %s!\n", ps.ByName("name"))
}
