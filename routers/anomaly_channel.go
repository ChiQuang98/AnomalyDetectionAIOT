package routers

import (
	"AnomalyDetection/controllers"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
)

func SetChannelConfig(router *mux.Router) *mux.Router {
	router.Handle("/anomalyconfig",
		negroni.New(
			negroni.HandlerFunc(controllers.AnomalyConfig),
		)).Methods("POST")
	return router
}

