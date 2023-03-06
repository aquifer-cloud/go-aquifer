package aquifer

import (
    "fmt"
    "time"
    "regexp"
    "strings"
    "encoding/json"
)

func DiscoverSchema(records []map[string]interface{}) (jsonSchema map[string]interface{}, err error) {
    path := []string{}
    for i, value := range records {
        var recordJsonSchema map[string]interface{}
        recordJsonSchema, err = discoverValueSchema(path, value)
        if err != nil {
            return
        }
        if i == 0 {
            jsonSchema = recordJsonSchema
        } else {
            jsonSchema, err = mergeSchemas(path, jsonSchema, recordJsonSchema)
            if err != nil {
                return
            }
        }
    }
    err = defaultNullSchemas(jsonSchema)
    return
}

func defaultNullSchemas(jsonSchema map[string]interface{}) (err error) {
    for _, rawPropSchema := range jsonSchema["properties"].(map[string]interface{}) {
        propSchema := rawPropSchema.(map[string]interface{})
        rawJsonType := propSchema["type"]
        var primaryJsonType string
        switch rawJsonType.(type) {
        case string:
            primaryJsonType = rawJsonType.(string)
            if rawJsonType.(string) == "" || rawJsonType.(string) == "null" {

                propSchema["type"] = []string{"null", "string"}
            }
        case []string:
            for _, jsonType := range rawJsonType.([]string) {
                if jsonType != "null" {
                    primaryJsonType = jsonType
                }
            }
        default:
            err = fmt.Errorf("Cannot determine schema type")
            return
        }
        if primaryJsonType == "" {
            propSchema["type"] = []string{"null", "string"}
        } else if primaryJsonType == "object" {
            err = defaultNullSchemas(propSchema)
            if err != nil {
                return
            }
        } else if primaryJsonType == "array" {
            err = defaultNullSchemas(propSchema["items"].(map[string]interface{}))
            if err != nil {
                return
            }
        }
    }
    return
}

func discoverObjectSchema(path []string, value map[string]interface{}) (jsonSchema map[string]interface{}, err error) {
    properties := make(map[string]interface{})
    for key, value := range value {
        var propJsonSchema map[string]interface{}
        subSchema := append(path, key)
        propJsonSchema, err = discoverValueSchema(subSchema, value)
        if err != nil {
            return
        }
        properties[key] = propJsonSchema
    }
    jsonSchema = map[string]interface{}{
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
    }
    return
}

func discoverArraySchema(path []string, value []interface{}) (jsonSchema map[string]interface{}, err error) {
    var itemsSchema map[string]interface{}
    for i, value := range value {
        var itemJsonSchema map[string]interface{}
        itemJsonSchema, err = discoverValueSchema(path, value)
        if err != nil {
            return
        }
        if i == 0 {
            itemsSchema = itemJsonSchema
        } else {
            itemsSchema, err = mergeSchemas(path, itemsSchema, itemJsonSchema)
            if err != nil {
                return
            }
        }
    }
    jsonSchema = map[string]interface{}{
        "type": "array",
        "items": itemsSchema,
    }
    return
}

func discoverValueSchema(path []string, value interface{}) (jsonSchema map[string]interface{}, err error) {
    switch value.(type) {
    case map[string]interface{}:
        jsonSchema, err = discoverObjectSchema(path, value.(map[string]interface{}))
    case []interface{}:
        jsonSchema, err = discoverArraySchema(path, value.([]interface{}))
    default:
        var jsonType string
        var jsonFormat string
        jsonType, jsonFormat, err = discoverScalarSchema(path, value)
        if err != nil {
            return
        }
        jsonSchema = map[string]interface{}{
            "type": jsonType,
        }
        if jsonFormat != "" {
            jsonSchema["format"] = jsonFormat
        }
    }
    return
}

func discoverScalarSchema(path []string, value interface{}) (jsonType string, jsonFormat string, err error) {
    switch value.(type) {
    case int, int8, int32, int64:
        jsonType = "integer"
    case float32, float64:
        jsonType = "number"
    case json.Number:
        jsonNumber := value.(json.Number)
        var parseErr error
        _, parseErr = jsonNumber.Int64()
        if parseErr != nil {
            _, parseErr = jsonNumber.Float64()
            if parseErr != nil {
                err = parseErr
                return
            }
            jsonType = "number"
        } else {
            jsonType = "integer"
        }
    case time.Time:
        jsonType = "string"
        jsonFormat = "date-time"
    case bool:
        jsonType = "boolean"
    case nil:
        jsonType = "null"
    case string:
        jsonType, jsonFormat = getStringType(value.(string))
    default:
        err = fmt.Errorf("Type not supported for JSON Schema discovery: %T", value)
    }
    return
}

var IntegerRegex *regexp.Regexp = regexp.MustCompile("^[\\-|\\+]?[0-9]+$")
var NumberRegex *regexp.Regexp = regexp.MustCompile("^[\\-|\\+]?[0-9]*\\.[0-9]+$")
var BooleanRegex *regexp.Regexp = regexp.MustCompile("^(true|True|TRUE|T|t|false|False|FALSE|F|f)$")
var ISO8601DateRegex *regexp.Regexp = regexp.MustCompile("^(([12]\\d{3})-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01]))$")
var ISO8601Regex *regexp.Regexp = regexp.MustCompile("^(([12]\\d{3})-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01]))(T|\\s)(2[0-3]|[01][0-9]):?([0-5][0-9]):?([0-5][0-9])(\\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9])(?::?(?:[0-5][0-9]))?)?$")

func getStringType(value string) (jsonType string, jsonFormat string) {
    value = strings.ToLower(strings.TrimSpace(value))

    jsonType = "string"
    if value == "" || value == "null" {
        jsonType = "null"
    } else if IntegerRegex.MatchString(value) {
        jsonType = "integer"
    } else if NumberRegex.MatchString(value) {
        jsonType = "number"
    } else if BooleanRegex.MatchString(value) {
        jsonType = "boolean"
    } else if ISO8601DateRegex.MatchString(value) {
        jsonFormat = "date"
    } else if ISO8601Regex.MatchString(value) {
        jsonFormat = "date-time"
    }
    return
}

func getType(path []string, rawJsonType interface{}) (primaryType string, nullable bool, err error) {
    var jsonType []string
    switch rawJsonType.(type) {
    case string:
        jsonType = []string{rawJsonType.(string)}
    case []string:
        jsonType = rawJsonType.([]string)
    case []interface{}:
        rawJsonTypeSlice := rawJsonType.([]interface{})
        jsonType = make([]string, len(rawJsonTypeSlice))
        for i, rawType := range rawJsonTypeSlice {
            typeStr, assertionOk := rawType.(string)
            if !assertionOk {
                err = fmt.Errorf("JSON Schema `type` must be an array of strings")
                return
            }
            jsonType[i] = typeStr
        }
    default:
        err = fmt.Errorf("JSON Schema `type` must be a string or array of strings")
        return
    }

    if len(jsonType) > 2 {
        err = fmt.Errorf("Cannot have more than one primary JSON type: %v %v", path, jsonType)
        return
    }
    for _, subType := range jsonType {
        if subType == "null" {
            nullable = true
        } else if primaryType != "" {
            err = fmt.Errorf("Cannot have multiple non-null JSON types: %v %v", path, jsonType)
            return
        } else {
            primaryType = subType
        }
    }
    return
}

func mergeSchemas(path []string, jsonSchemaA map[string]interface{}, jsonSchemaB map[string]interface{}) (jsonSchema map[string]interface{}, err error) {
    var aJsonType string
    var aNullable bool
    aJsonType, aNullable, err = getType(path, jsonSchemaA["type"])
    if err != nil {
        return
    }

    var bJsonType string
    var bNullable bool
    bJsonType, bNullable, err = getType(path, jsonSchemaB["type"])
    if err != nil {
        return
    }

    // if one is only null, while the other is not
    if aJsonType == "" {
        aJsonType = bJsonType
    }
    if bJsonType == "" {
        bJsonType = aJsonType
    }

    if aJsonType == "object" && bJsonType != "object" {
        err = fmt.Errorf("Cannot merge object with non-object: %v", path)
        return
    }
    if aJsonType == "array" && bJsonType != "array" {
        err = fmt.Errorf("Cannot merge array with non-array: %v", path)
        return
    }

    if aJsonType == "object" {
        outProperties := make(map[string]interface{})
        aProperties := jsonSchemaA["properties"].(map[string]interface{})
        bProperties := jsonSchemaB["properties"].(map[string]interface{})

        for aKey, aValue := range aProperties {
            aValueJsonSchema := aValue.(map[string]interface{})
            bValue, bExists := bProperties[aKey]
            subPath := append(path, aKey)
            var subSchema map[string]interface{}
            if bExists {
                subSchema, err = mergeSchemas(subPath, aValueJsonSchema, bValue.(map[string]interface{}))
                if err != nil {
                    return
                }
            } else {
                subSchema = aValueJsonSchema
            }
            outProperties[aKey] = subSchema
        }

        for bKey, bValue := range bProperties {
            _, aExists := aProperties[bKey]
            if !aExists {
                outProperties[bKey] = bValue
            }
            
        }
        jsonSchema = map[string]interface{}{
            "type": "object",
            "additionalProperties": false,
            "properties": outProperties,
        }
    } else if aJsonType == "array" {
        var itemsSchema map[string]interface{}
        itemsSchema, err = mergeSchemas(path, jsonSchemaA["items"].(map[string]interface{}), jsonSchemaB["items"].(map[string]interface{}))
        if err != nil {
            return
        }
        jsonSchema = map[string]interface{}{
            "type": "array",
            "items": itemsSchema,
        }
    } else if aJsonType != "" { // both should either bull blank or not blank at this point
        var outJsonType string
        var outJsonFormat string
        if bJsonType == "" { // it was null / blank
            outJsonType = aJsonType
        } else if aJsonType == "integer" && bJsonType == "integer" {
            outJsonType = "integer"
        } else if (aJsonType == "integer" || aJsonType == "number") &&
                  (bJsonType == "integer" || bJsonType == "number") {
            outJsonType = "number"
        } else if aJsonType == "boolean" && bJsonType == "boolean" {
            outJsonType = "boolean"
        } else {
            outJsonType = "string"

            aFormat, aFormatExists := jsonSchemaA["format"]
            bFormat, bFormatExists := jsonSchemaB["format"]
            if aFormatExists && bFormatExists && aFormat.(string) == bFormat.(string) {
                outJsonFormat = aFormat.(string)
            }
        }
        jsonSchema = map[string]interface{}{
            "type": outJsonType,
        }
        if outJsonFormat != "" {
            jsonSchema["format"] = outJsonFormat
        }
    } else { // both are blank
        jsonSchema = map[string]interface{}{
            "type": "null",
        }
        return
    }

    if aNullable || bNullable {
        jsonSchema["type"] = []string{"null", jsonSchema["type"].(string)}
    }
    return
}
