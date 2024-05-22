package aquifer

import (
    "fmt"
    "time"
    "strings"
    "reflect"
    "strconv"
    "encoding/json"

    "golang.org/x/exp/slices"
)

type TransformHandlers map[string]func(map[string]interface{}, interface{}) (interface{}, error)

func Transform(schema map[string]interface{},
               obj map[string]interface{},
               handlers ...TransformHandlers) (map[string]interface{}, error) {
    if len(handlers) > 0 {
        return transformObject([]string{}, schema, obj, handlers[0])
    } else {
        handlersMap := make(TransformHandlers)
        return transformObject([]string{}, schema, obj, handlersMap)
    }
}

func jsonSchemaError(path []string, msg ...any) error {
    msgStr := ""
    if len(msg) > 0 {
        msgArg := msg[0].(string)
        msgStr = " - " + fmt.Sprintf(msgArg, msg[1:]...)
    }
    return fmt.Errorf("JSON Schema invalid%s: \"%s\"",
                      msgStr,
                      strings.Join(path, "\",\""))
}

func transformObject(path []string,
                     schema map[string]interface{},
                     obj map[string]interface{},
                     handlers TransformHandlers) (out map[string]interface{}, err error) {
    var (
        exists bool
        selected bool
        value interface{}
        propertiesRaw interface{}
    )
    propertiesRaw, exists = schema["properties"]
    if !exists {
        out = obj
        return
    }
    out = make(map[string]interface{})
    properties, assertionOk := propertiesRaw.(map[string]interface{})
    if exists && assertionOk {
        for fieldName, fieldSchemaRaw := range properties {
            newPath := append(path, fieldName)
            var fieldSchema map[string]interface{}
            fieldSchema, assertionOk = fieldSchemaRaw.(map[string]interface{})
            if !assertionOk {
                err = jsonSchemaError(newPath)
                return
            }
            var selectedRaw interface{}
            selectedRaw, exists = fieldSchema["selected"]
            if exists {
                selected, assertionOk = selectedRaw.(bool)
                if !assertionOk {
                    err = jsonSchemaError(newPath, "\"selected\" must be a boolean")
                    return
                }
                if selected == false {
                    continue
                }
            }
            var keyHandler func(map[string]interface{}, interface{}) (interface{}, error)
            keyHandler, exists = handlers["key"]
            // TODO: memoize keys?
            if exists {
                var keyRaw interface{}
                keyRaw, err = keyHandler(fieldSchema, fieldName)
                if err != nil {
                    return
                }
                var key string
                key, assertionOk = keyRaw.(string)
                if !assertionOk {
                    err = fmt.Errorf("Transform key handler must return a string")
                    return
                }
                value, exists = obj[key]
            } else {
                value, exists = obj[fieldName]
            }
            if exists {
                if value != nil {
                    value, err = transformValue(
                        newPath,
                        fieldSchema,
                        value,
                        handlers)
                    if err != nil {
                        return
                    }
                }
                out[fieldName] = value
            }
        }
    } else {
        err = jsonSchemaError(path, "Object schemas must have a \"properties\" field")
        return
    }
    return
}

func transformValue(path []string,
                    schema map[string]interface{},
                    value interface{},
                    handlers TransformHandlers) (out interface{}, err error) {
    complexTypeRaw, complexTypeExists := schema["complexType"]
    if complexTypeExists {
        complexType, assertionOk := complexTypeRaw.(string)
        if !assertionOk {
            err = jsonSchemaError(path, "\"complexType\" must be a string")
            return
        }
        if handler, handlerExists := handlers[complexType]; handlerExists {
            return handler(schema, value)
        }
    }

    formatRaw, formatExists := schema["format"]
    if formatExists {
        format, assertionOk := formatRaw.(string)
        if !assertionOk {
            err = jsonSchemaError(path, "\"format\" must be a string")
            return
        }
        if handler, handlerExists := handlers[format]; handlerExists {
            return handler(schema, value)
        }
        switch format {
        case "date":
            value, err = time.Parse("2006-01-02", strings.Split(value.(string), "T")[0])
            if err != nil {
                return
            }
        case "date-time":
            value, err = time.Parse(time.RFC3339Nano, value.(string))
            if err != nil {
                return
            }
        }
    }

    if jsonTypeRaw, jsonTypeExists := schema["type"]; jsonTypeExists {
        var jsonType []string
        switch jsonTypeRaw.(type) {
        case string:
            jsonType = []string{jsonTypeRaw.(string)}
            break
        case []interface{}:
            jsonTypeArray := jsonTypeRaw.([]interface{})
            jsonType = make([]string, len(jsonTypeArray))
            for i, typeItem := range jsonTypeArray {
                typeStr, assertionOk := typeItem.(string)
                if !assertionOk {
                    err = jsonSchemaError(path, "\"type\" must be a string or array of strings")
                    return
                }
                jsonType[i] = typeStr
            }
        case []string:
            tmpJsonType := jsonTypeRaw.([]string)
            jsonType = make([]string, len(tmpJsonType))
            copy(jsonType, tmpJsonType)
        default:
            err = jsonSchemaError(path, "\"type\" must be a string or array of strings")
            return
        }
        if nullIndex := slices.Index(jsonType, "null"); nullIndex > -1 {
            jsonType = slices.Delete(jsonType, nullIndex, nullIndex + 1)
        }
        if len(jsonType) == 0 {
            err = jsonSchemaError(path, "\"type\" must have one non-null type")
            return
        }
        if len(jsonType) > 1 {
            err = jsonSchemaError(path, "\"type\" must have exactly one non-null type")
            return
        }
        primaryJsonType := jsonType[0]
        if handler, handlerExists := handlers[primaryJsonType]; handlerExists {
            return handler(schema, value)
        }
        if primaryJsonType == "object" || primaryJsonType == "array" {
            switch value.(type) {
            case string:
                err = Unmarshal([]byte(value.(string)), &value)
                if err != nil {
                    return
                }
            case []byte:
                err = Unmarshal(value.([]byte), &value)
                if err != nil {
                    return
                }
            }
        }
        if primaryJsonType == "object" {
            valueMap, assertionOk := value.(map[string]interface{})
            if !assertionOk {
                err = jsonSchemaError(path, "Object expected")
                return
            }
            return transformObject(path, schema, valueMap, handlers)
        } else if primaryJsonType == "array" {
            itemsRaw, itemsExists := schema["items"]
            if !itemsExists {
                err = jsonSchemaError(path, "Array type without \"items\"")
                return
            }
            newPath := append(path, "items")
            itemsSchema, assertionOk := itemsRaw.(map[string]interface{})
            if !assertionOk {
                err = jsonSchemaError(newPath, "\"items\" should be an object")
                return
            }
            itemType := reflect.TypeOf(value)
            if itemType.Kind() == reflect.Slice {
                valueArray := value.([]interface{})
                outArray := make([]interface{}, len(valueArray))
                for i, item := range valueArray {
                    outArray[i], err = transformValue(newPath, itemsSchema, item, handlers)
                    if err != nil {
                        return
                    }
                }
                out = outArray
            } else {
                value, err = transformValue(newPath, itemsSchema, value, handlers)
                if err != nil {
                    return
                }
                out = []interface{}{value}
            }
            return
        } else {
            return transformScalar(path,
                                   primaryJsonType,
                                   value)
        }
    } else {
        err = jsonSchemaError(path, "No JSON Schema `type`")
        return
    }
    // TODO: anyOf, allOf, and oneOf
    // exhausted all valid options
    err = jsonSchemaError(path, "Invalid value schema")
    return
}

func transformScalar(path []string,
                     jsonType,
                     value interface{}) (out interface{}, err error) {
    if value == nil {
        return
    }

    switch value.(type) {
    case string:
        valueStr := value.(string)
        valueTrimmed := strings.TrimSpace(valueStr)
        if valueTrimmed == "" {
            return
        }
        switch jsonType {
        case "string":
            out = valueStr
        case "integer":
            out, err = strconv.Atoi(valueStr)
        case "number":
            out, err = strconv.ParseFloat(valueStr, 64)
        case "boolean":
            out, err = strconv.ParseBool(valueStr)
        default:
            err = fmt.Errorf("Incompatible transformation types: %v %s %T", path, jsonType, value)
        }
    case json.Number:
        valueJson := value.(json.Number)
        switch jsonType {
        case "string":
            out = valueJson.String()
        case "integer":
            var tmpInt int64
            tmpInt, err = valueJson.Int64()
            if err != nil {
                return
            }
            out = int(tmpInt)
        case "number":
            out, err = valueJson.Float64()
        default:
            err = fmt.Errorf("Incompatible transformation types: %v %s %T", path, jsonType, value)
        }
    case int, int8, int32, int64:
        switch jsonType {
        case "string":
            out, err = ValueToString(value)
        case "integer", "number":
            out = value
        default:
            err = fmt.Errorf("Incompatible transformation types: %v %s %T", path, jsonType, value)
        }
    case float32, float64:
        switch jsonType {
        case "string":
            out, err = ValueToString(value)
        case "integer":
            switch value.(type) {
            case float32:
                out = int(value.(float32))
            case float64:
                out = int(value.(float64))
            }
        case "number":
            out = value
        default:
            err = fmt.Errorf("Incompatible transformation types: %v %s %T", path, jsonType, value)
        }
    case bool:
        switch jsonType {
        case "string":
            if value.(bool) == true {
                out = "true"
            } else {
                out = "false"
            }
        case "boolean":
            out = value
        default:
            err = fmt.Errorf("Incompatible transformation types: %v %s %T", path, jsonType, value)
        }
    }
    return
}

func ValueToString(value interface{}) (interface{}, error) {
    switch value.(type) {
    case string:
        str := value.(string)
        if str == "" {
            return nil, nil
        }
        return str, nil
    case time.Time:
        return value.(time.Time).Format(time.RFC3339Nano), nil
    default:
        b, err := json.Marshal(value)
        if err != nil {
            return nil, nil
        }
        return string(b), nil
    }
}
