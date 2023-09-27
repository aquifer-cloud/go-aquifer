package aquifer

import (
	"encoding/json"
)

type Dict map[string]interface{}

func ToDict(item interface{}) Dict {
	return Dict(item.(map[string]interface{}))
}

func (d Dict) Copy() Dict {
	cp := make(Dict)
	for k, v := range d {
		vm, ok := v.(Dict)
		if ok {
			cp[k] = vm.Copy()
		} else {
			cp[k] = v
		}
	}
	return cp
}

func (d Dict) Merge(d2 Dict) Dict {
	d1 := d.Copy()
	for key, value := range d2 {
		d1[key] = value
	}
	return d1
}

func (d Dict) Get(k string) Dict {
   v, exists := d[k]
	if exists && v != nil {
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
	if exists && v != nil {
		return v.([]interface{})
	} else {
		return make([]interface{}, 0)
	}
}

func (d Dict) GetStringArray(k string) []string {
   v, exists := d[k]
	if exists && v != nil {
		return v.([]string)
	} else {
		return make([]string, 0)
	}
}

func (d Dict) GetMapArray(k string) []map[string]interface{} {
   v, exists := d[k]
	if exists && v != nil {
		return v.([]map[string]interface{})
	} else {
		return make([]map[string]interface{}, 0)
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
		case float64:
			v = int(rawValue.(float64))
		case int64:
			v = int(rawValue.(int64))
		case json.Number:
			tmp, _ := rawValue.(json.Number).Int64()
			v = int(tmp)
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
	if exists && rawValue != nil {
		switch rawValue.(type) {
		case int:
			v = float64(rawValue.(int))
		case float64:
			v = rawValue.(float64)
		case int64:
			v = float64(rawValue.(int64))
		case json.Number:
			v, _ = rawValue.(json.Number).Float64()
		}
	}
	return
}

func (d Dict) SetFloat64(k string, v float64) Dict {
   d[k] = v
   return d
}

func (d Dict) GetBool(k string) (v bool) {
	rawValue, exists := d[k]
	if exists && rawValue != nil {
		v = rawValue.(bool)
	}
	return
}

func (d Dict) SetBool(k string, v bool) Dict {
   d[k] = v
   return d
}

func (d Dict) Map() map[string]interface{} {
	return d
}

func (d Dict) SetAny(k string, v any) Dict {
	d[k] = v
	return d
}
