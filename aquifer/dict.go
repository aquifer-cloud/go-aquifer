package aquifer

import (
	"encoding/json"
)

type Dict map[string]interface{}

func (d Dict) Get(k string) Dict {
   v, exists := d[k]
	if exists {
		return v.(map[string]interface{})
	} else {
		return make(map[string]interface{})
	}
}

func (d Dict) Set(k string, v map[string]interface{}) Dict {
   d[k] = v
   return d
}

func (d Dict) GetArray(k string) []interface{} {
   v, exists := d[k]
	if exists {
		return v.([]interface{})
	} else {
		return make([]interface{}, 0)
	}
}

func (d Dict) SetArray(k string, v interface{}) Dict {
   d[k] = v
   return d
}

func (d Dict) GetString(k string) string {
	v, exists := d[k]
	if exists && v != nil {
		return v.(string)
	} else {
		return ""
	}
}

func (d Dict) SetString(k string, v string) Dict {
   d[k] = v
   return d
}

func (d Dict) GetInt(k string) (v int, exists bool) {
	var rawValue interface{}
	rawValue, exists = d[k]
	if exists {
		switch rawValue.(type) {
		case int:
			v = rawValue.(int)
			break
		case float64:
			v = int(rawValue.(float64))
			break
		case int64:
			v = int(rawValue.(int64))
			break
		case json.Number:
			tmp, _ := rawValue.(json.Number).Int64()
			v = int(tmp)
			break
		}
	}
	return
}

func (d Dict) SetInt(k string, v int) Dict {
   d[k] = v
   return d
}

func (d Dict) GetFloat64(k string) (v float64, exists bool) {
	var rawValue interface{}
	rawValue, exists = d[k]
	if exists {
		v = rawValue.(float64)
	}
	return
}

func (d Dict) SetFloat64(k string, v float64) Dict {
   d[k] = v
   return d
}

func (d Dict) Map() map[string]interface{} {
	return d
}
