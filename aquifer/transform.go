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
    out = make(map[string]interface{})
    var (
        exists bool
        selected bool
        value interface{}
        propertiesRaw interface{}
    )
    propertiesRaw, exists = schema["properties"]
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
            value, exists = obj[fieldName]
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
                outArray := make([]interface{}, itemType.Len())
                for i, item := range value.([]interface{}) {
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
        } else {
            return transformScalar(path,
                                   primaryJsonType,
                                   value)
        }
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

    switch jsonType {
    case "string":
        out, err = ValueToString(value)
        return
    case "integer":
        if num, ok := value.(int); ok {
            out = num
            return
        }

        if num, ok := value.(int64); ok {
            out = int(num)
            return
        }

        if num, ok := value.(float64); ok {
            out = int(num)
            return
        }

        if str, ok := value.(string); ok {
            out, err = strconv.Atoi(str)
            return
        }

        err = fmt.Errorf("Cannot convert value to integer: %T %v", value, value)
        return
    case "number":
        if num, ok := value.(float64); ok {
            out = num
            return
        }

        if num, ok := value.(int); ok {
            out = float64(num)
            return
        }

        if str, ok := value.(string); ok {
            out, err = strconv.ParseFloat(str, 64)
            return
        }

        err = fmt.Errorf("Cannot convert value to float: %T %v", value, value)
        return
    case "boolean":
        if b, ok := value.(bool); ok {
            out = b
            return
        }

        // formating as string handles integers too, eg 1 and 0
        out, err = strconv.ParseBool(fmt.Sprintf("%v", value))
        if err != nil {
            return
        }

        err = fmt.Errorf("Cannot convert value to boolean: %T %v", value, value)
        return
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
