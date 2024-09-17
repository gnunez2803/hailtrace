package weather

import (
	"time"

	sq "github.com/Masterminds/squirrel"
)

type WeatherDbEvent interface {
	Save(dbRepo *MysqlRepository) error
}

type WindEvent struct {
	EventTime time.Time `json:"event_time"`
	Speed     string    `json:"speed"`
	Location  string    `json:"location"`
	County    string    `json:"county"`
	State     string    `json:"state"`
	Lat       float64   `json:"lat"`
	Lon       float64   `json:"lon"`
	Comments  string    `json:"comments"`
}

func (w WindEvent) Save(dbRepo *MysqlRepository) error {
	sqlQuery := sq.Insert("wind_events").Columns("event_time", "speed", "location", "county", "state", "lat", "lon", "comments").
		Values(w.EventTime, w.Speed, w.Location, w.County, w.State, w.Lat, w.Lon, w.Comments)
	stm, args, err := sqlQuery.ToSql()
	if err != nil {
		return err
	}
	_, err = dbRepo.DB.Exec(stm, args...)
	return err
}

type TornadoEvent struct {
	EventTime time.Time `json:"event_time"`
	FScale    string    `json:"f_scale"`
	Location  string    `json:"location"`
	County    string    `json:"county"`
	State     string    `json:"state"`
	Lat       float64   `json:"lat"`
	Lon       float64   `json:"lon"`
	Comments  string    `json:"comments"`
}

func (w TornadoEvent) Save(dbRepo *MysqlRepository) error {

	sqlQuery := sq.Insert("tornado_events").Columns("event_time", "f_scale", "location", "county", "state", "lat", "lon", "comments").
		Values(w.EventTime, w.FScale, w.Location, w.County, w.State, w.Lat, w.Lon, w.Comments)
	stm, args, err := sqlQuery.ToSql()
	if err != nil {
		return err
	}
	_, err = dbRepo.DB.Exec(stm, args...)
	return err
}

type HailEvent struct {
	EventTime time.Time `json:"event_time"`
	Size      string    `json:"size"`
	Location  string    `json:"location"`
	County    string    `json:"county"`
	State     string    `json:"state"`
	Lat       float64   `json:"lat"`
	Lon       float64   `json:"lon"`
	Comments  string    `json:"comments"`
}

func (w HailEvent) Save(dbRepo *MysqlRepository) error {
	sqlQuery := sq.Insert("hail_events").Columns("event_time", "size", "location", "county", "state", "lat", "lon", "comments").
		Values(w.EventTime, w.Size, w.Location, w.County, w.State, w.Lat, w.Lon, w.Comments)
	stm, args, err := sqlQuery.ToSql()
	if err != nil {
		return err
	}
	_, err = dbRepo.DB.Exec(stm, args...)
	return err
}

type ApiResponse struct {
	TotalElements int            `json:"total_elements"`
	HEvents       []HailEvent    `json:"hail_events"`
	TEvents       []TornadoEvent `json:"tornado_events"`
	WEvents       []WindEvent    `json:"wind_events"`
}

type ModelsRepo struct {
	DbRepo *MysqlRepository
}

func NewModelsRepo(db *MysqlRepository) ModelsRepo {
	return ModelsRepo{
		DbRepo: db,
	}
}

func (m ModelsRepo) fetchHailStorms(location string, date string) ([]HailEvent, error) {
	sql := sq.Select("event_time", "size", "location", "county", "state", "lat", "lon", "comments").
		From("hail_events")
	if date != "" {
		sql = sql.Where(sq.Expr("DATE(event_time) = ?", date))
	}

	// Add condition for location if provided
	if location != "" {
		sql = sql.Where(sq.Eq{"location": location})
	}
	sqlQuery, args, err := sql.ToSql()

	if err != nil {
		return []HailEvent{}, err
	}
	rows, err := m.DbRepo.DB.Query(sqlQuery, args...)
	if err != nil {
		return []HailEvent{}, err
	}
	var events []HailEvent
	defer rows.Close()
	for rows.Next() {
		var result HailEvent
		var eventTimeStr string

		err = rows.Scan(
			&eventTimeStr,
			&result.Size,
			&result.Location,
			&result.County,
			&result.State,
			&result.Lat,
			&result.Lon,
			&result.Comments,
		)
		eventTime, err := time.Parse("2006-01-02 15:04:05", eventTimeStr)
		if err != nil {
			return []HailEvent{}, err
		}
		result.EventTime = eventTime
		events = append(events, result)
	}
	if rows.Err() != nil {
		return []HailEvent{}, err
	}
	return events, nil
}

func (m ModelsRepo) fetchWindStorms(location string, date string) ([]WindEvent, error) {
	sql := sq.Select("event_time", "speed", "location", "county", "state", "lat", "lon", "comments").
		From("wind_events")
	if date != "" {
		sql = sql.Where(sq.Expr("DATE(event_time) = ?", date))
	}
	if location != "" {
		sql = sql.Where(sq.Eq{"location": location})
	}
	sqlQuery, args, err := sql.ToSql()

	if err != nil {
		return []WindEvent{}, err
	}
	rows, err := m.DbRepo.DB.Query(sqlQuery, args...)
	if err != nil {
		return []WindEvent{}, err
	}
	var events []WindEvent
	defer rows.Close()
	for rows.Next() {
		var result WindEvent
		var eventTimeStr string
		err = rows.Scan(
			&eventTimeStr,
			&result.Speed,
			&result.Location,
			&result.County,
			&result.State,
			&result.Lat,
			&result.Lon,
			&result.Comments,
		)
		eventTime, err := time.Parse("2006-01-02 15:04:05", eventTimeStr)
		if err != nil {
			return []WindEvent{}, err
		}
		result.EventTime = eventTime
		events = append(events, result)
	}
	if rows.Err() != nil {
		return []WindEvent{}, err
	}
	return events, nil
}

func (m ModelsRepo) fetchTornadoStorms(location string, date string) ([]TornadoEvent, error) {
	sql := sq.Select("event_time", "size", "location", "county", "state", "lat", "lon", "comments").
		From("hail_events")
	if date != "" {
		sql = sql.Where(sq.Expr("DATE(event_time) = ?", date))
	}
	if location != "" {
		sql = sql.Where(sq.Eq{"location": location})
	}
	sqlQuery, args, err := sql.ToSql()

	if err != nil {
		return []TornadoEvent{}, err
	}
	rows, err := m.DbRepo.DB.Query(sqlQuery, args...)
	if err != nil {
		return []TornadoEvent{}, err
	}
	var events []TornadoEvent
	defer rows.Close()
	for rows.Next() {
		var result TornadoEvent
		var eventTimeStr string
		err = rows.Scan(
			&eventTimeStr,
			&result.FScale,
			&result.Location,
			&result.County,
			&result.State,
			&result.Lat,
			&result.Lon,
			&result.Comments,
		)
		eventTime, err := time.Parse("2006-01-02 15:04:05", eventTimeStr)
		if err != nil {
			return []TornadoEvent{}, err
		}
		result.EventTime = eventTime
		events = append(events, result)
	}
	if rows.Err() != nil {
		return []TornadoEvent{}, err
	}
	return events, nil
}

func (x *ModelsRepo) GetStorms(location string, date string) (ApiResponse, error) {
	hailStorms, err := x.fetchHailStorms(location, date)
	if err != nil {
		return ApiResponse{}, err
	}
	windStorms, err := x.fetchWindStorms(location, date)
	if err != nil {
		return ApiResponse{}, err
	}

	tornadoStorms, err := x.fetchTornadoStorms(location, date)
	if err != nil {
		return ApiResponse{}, err
	}
	totalElements := len(hailStorms) + len(windStorms) + len(tornadoStorms)
	apiResponse := ApiResponse{
		TotalElements: totalElements,
		HEvents:       hailStorms,
		WEvents:       windStorms,
		TEvents:       tornadoStorms,
	}
	return apiResponse, nil
}
