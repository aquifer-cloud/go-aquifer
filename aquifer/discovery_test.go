package aquifer

import (
    "fmt"
    "testing"
)

func TestDiscovery(t *testing.T) {
    records := []map[string]interface{}{
        map[string]interface{}{
            "id": "123",
            "some_int": 345,
            "some_float": "23.32222",
            "some_date_str": "2018-01-23",
            "some_date_str_fake": "2018-01-03",
            "some_datetime_str": "2018-01-23 09:12:22",
            "some_datetime_str2": "2018-01-23T09:12:22.3344",
            "some_bool": true,
            "some_other_bool": "true",
            "some_other_bool_fake": "true",
        },
        map[string]interface{}{
            "id": "123",
            "some_int": 345,
            "some_float": "23.322",
            "some_date_str": "2018-01-23",
            "some_date_str_fake": "dogs",
            "some_datetime_str": "2018-01-23T09:12:22",
            "some_datetime_str2": "2018-01-23T09:12:22+08:00",
            "some_bool": "false",
            "some_other_bool": "t",
            "some_other_bool_fake": "gh",
            "some_nested_obj": map[string]interface{}{
                "some_nested_field": "foo",
                "some_nested_field_datetime": "2019-01-23T03:14:22",
            },
        },
        map[string]interface{}{
            "id": "1277",
            "some_float": "23.322",
            "some_date_str": "2018-01-23",
            "some_date_str_fake": "dogs",
            "some_datetime_str": "2018-01-23T09:12:22",
            "some_datetime_str2": "2018-01-23T09:12:22+08:00",
            "some_bool": "false",
            "some_other_bool": nil,
            "some_other_bool_fake": "gh",
            "some_nested_array": []interface{}{
                map[string]interface{}{
                    "some_nested_field": "foo",
                    "some_nested_field_datetime": "2019-01-23T03:14:22",
                },
                map[string]interface{}{
                    "some_nested_field": "foo",
                    "some_nested_field_datetime": "2019-01-23",
                },
            },
        },
    }

    jsonSchema, err := DiscoverSchema(records)
    fmt.Println(err)
    fmt.Println(jsonSchema)
}
