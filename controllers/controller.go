package controllers

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

type ControllerType struct {
	W  http.ResponseWriter
	R  *http.Request
	Ps httprouter.Params
}
