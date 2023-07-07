package es

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/beego/beego/v2/adapter/httplib"
)

var esUrl string

func init() {
	//9200后面的/不要少
	esUrl = "http://127.0.0.1:9200/"
}

// CreateIndex 编写该方法实现Es索引创建，并指定分词规则为ik分词
func CreateIndex(indexName string, body interface{}) bool {
	//httplib请求包
	req := httplib.Put(esUrl + indexName)
	//建立索引配置
	req.JSONBody(body)
	//req.String()这个不能少
	str, err := req.String()
	if err != nil {
		return false
	}
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(str)
	return true
}

func CreateMapping(indexName string, body interface{}) bool {
	//httplib请求包
	req := httplib.Put(esUrl + indexName + "/_mapping")

	//设置请求头
	req.Header("Content-Type", "application/json;charset=utf-8")

	req.JSONBody(body)

	//req.String()这个不能少
	str, err := req.String()
	if err != nil {
		return false
	}
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(str)
	return true
}

func EsAdd(indexName string, body interface{}, id int) bool {
	//httplib请求包
	req := httplib.Post(esUrl + indexName + "/_doc/" + strconv.Itoa(id))
	//参数通过json的格式传过去
	req.JSONBody(body)

	//req.String()这个不能少
	str, err := req.String()
	if err != nil {
		return false
	}
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(str)
	return true
}

func EsSearch(indexName string, query interface{}, from int, size int, sort interface{}) ReqSearchData {
	body := map[string]interface{}{
		"query": query,
		"from":  from,
		"size":  size,
		"sort":  sort,
	}
	//httplib请求包
	req := httplib.Post(esUrl + indexName + "/_search")
	//参数通过json的格式传过去
	req.JSONBody(body)

	//req.String()这个不能少
	str, err := req.String()
	reqData := ReqSearchData{}

	json.Unmarshal([]byte(str), &reqData)
	if err != nil {
		fmt.Println(err)
	}

	return reqData

}

// 解析获取到的值
type ReqSearchData struct {
	Hits HitsData `json:"hits"`
}

type HitsData struct {
	Total TotalData     `json:"total"`
	Hits  []HitsTwoData `json:"hits"`
}

type TotalData struct {
	Value    int
	Relation string
}

type HitsTwoData struct {
	Source    json.RawMessage `json:"_source"`
	Highlight json.RawMessage `json:"highlight"`
}

// EsHighlightSearch implements a highlighted search in Elasticsearch
func EsHighlightSearch(indexName string, query interface{}, from int, size int, sort interface{}, highlightFields map[string]interface{}) ReqSearchData {

	// Prepare the search body
	body := map[string]interface{}{
		"query":     query,
		"from":      from,
		"size":      size,
		"sort":      sort,
		"highlight": highlightFields,
	}

	// httplib request package
	req := httplib.Post(esUrl + indexName + "/_search")
	// Pass parameters in JSON format
	req.JSONBody(body)

	// req.String() is necessary
	str, err := req.String()
	reqData := ReqSearchData{}

	json.Unmarshal([]byte(str), &reqData)
	if err != nil {
		fmt.Println(err)
	}

	return reqData
}

// EsBulkAdd 实现批量添加数据
func EsBulkAdd(indexName string, data []interface{}) bool {
	//httplib请求包
	req := httplib.Post(esUrl + indexName + "/_bulk")
	// 设置请求头Content-Type
	req.Header("Content-Type", "application/x-ndjson")

	//参数通过json的格式传过去
	var bulkData string

	for _, item := range data {
		//这段代码首先尝试将每个数据项序列化为 JSON 字符串。如果序列化过程有误，它将跳过该数据项并继续处理其它数据
		itemJSON, err := json.Marshal(item)
		if err != nil {
			fmt.Printf("Error marshaling data item: %s\n", err)
			continue
		}
		bulkData += fmt.Sprintf("{\"index\":{}}\n%s\n", string(itemJSON))
	}

	req.Body([]byte(bulkData))

	//req.String()这个不能少
	str, err := req.String()
	if err != nil {
		return false
	}
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(str)
	return true
}
