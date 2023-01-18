package aquifer

import (
    "fmt"
    "sort"
    "encoding/hex"
    "encoding/json"
    "encoding/binary"
    "crypto/sha256"
)

func Hash(schema map[string]interface{}, value map[string]interface{}) (hash string, err error) {
    var byteHash []byte
    byteHash, err = hashValue(schema, value)
    if err != nil {
        return
    }
    hash = hex.EncodeToString(byteHash)
    return
}

func hashValue(schema map[string]interface{}, value interface{}) (hash []byte, err error) {
    if value == nil {
        return
    }

    var assertionOk bool
    if excludeRaw, excludeExists := schema["hashExclude"]; excludeExists {
        var exclude bool
        exclude, assertionOk = excludeRaw.(bool)
        if !assertionOk {
            err = fmt.Errorf("`hashExclude` must be a boolean")
            return
        }
        if exclude == true {
            return
        }
    }

    switch value.(type) {
    case string:
        tmpHash := sha256.Sum256([]byte(value.(string)))
        hash = tmpHash[:]
    case json.Number:
        tmpHash := sha256.Sum256([]byte(value.(json.Number).String()))
        hash = tmpHash[:]
    case int:
    case int32:
    case int64:
    case float32:
    case float64:
    case bool:
        hasher := sha256.New()
        err = binary.Write(hasher, binary.LittleEndian, value)
        hash = hasher.Sum(nil)
    case map[string]interface{}:
        mapHash := make([]byte, 0)
        var itemHash []byte
        var mapSchema map[string]interface{}
        mapSchemaRaw, schemaExists := schema["properties"]
        if schemaExists {
            mapSchema, assertionOk = mapSchemaRaw.(map[string]interface{})
            if !assertionOk {
                err = fmt.Errorf("Object schema properties not an object")
                return
            }
        } else {
            mapSchema = make(map[string]interface{})
        }

        valueMap := value.(map[string]interface{})
        keys := make([]string, 0)
        for key := range valueMap {
            keys = append(keys, key)
        }
        sort.Strings(keys)

        for _, key := range keys {
            itemValue := valueMap[key]
            var itemSchema map[string]interface{}
            if schemaExists {
                var itemSchemaRaw interface{}
                itemSchemaRaw, schemaExists = mapSchema[key]
                if schemaExists {
                    itemSchema, assertionOk = itemSchemaRaw.(map[string]interface{})
                    if !assertionOk {
                        err = fmt.Errorf("Object schema not an object")
                        return
                    }
                }
            }
            if !schemaExists {
                itemSchema = make(map[string]interface{})
            }
            itemHash, err = hashValue(itemSchema, itemValue)
            if err != nil {
                return
            }
            mapHash = append(mapHash, itemHash...)
        }
        tmpHash := sha256.Sum256(mapHash)
        hash = tmpHash[:]
    case []interface{}:
        arrayHash := make([]byte, 0)
        var itemHash []byte
        var itemSchema map[string]interface{}
        arraySchema, schemaExists := schema["items"]
        if schemaExists {
            itemSchema, assertionOk = arraySchema.(map[string]interface{})
            if !assertionOk {
                err = fmt.Errorf("Object schema not an object")
                return
            }
        }
        if !schemaExists {
            itemSchema = make(map[string]interface{})
        }
        for _, itemValue := range value.([]interface{}) {
            itemHash, err = hashValue(itemSchema, itemValue)
            if err != nil {
                return
            }
            arrayHash = append(arrayHash, itemHash...)
        }
        tmpHash := sha256.Sum256(arrayHash)
        hash = tmpHash[:]
    default:
        err = fmt.Errorf("Object type not hashable: %T", value)
    }
    return
}
