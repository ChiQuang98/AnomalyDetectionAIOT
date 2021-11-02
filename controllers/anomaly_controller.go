package controllers

import (
	"AnomalyDetection/models"
	"AnomalyDetection/services"
	"encoding/json"
	"net/http"
)

func AnomalyConfig(w http.ResponseWriter, r *http.Request, n http.HandlerFunc) {
	anomaly_cfg := new(models.AnomalyChannel)
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(anomaly_cfg)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	} else {
		status, res := services.AnomalyChannel(anomaly_cfg)
		w.WriteHeader(status)
		if status == http.StatusOK {
			w.Write(res)
		}
	}
}
