package com.zengcode.elasticsearch.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.GetSourceResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RestController
public class ElasticSearchController {

    @Autowired
    private RestHighLevelClient client;

    @GetMapping(value = "/create")
    public String ping() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("users");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 2)
        );
        Map<String, Object> message = new HashMap<>();
        message.put("type", "text");

        Map<String, Object> properties = new HashMap<>();
        properties.put("userId", message);
        properties.put("name", message);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);

        request.mapping(mapping);
        CreateIndexResponse indexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        return "created";
    }

    @GetMapping(value = "/upsert")
    public String upsert() throws IOException {
        Map<String, Object> users = new HashMap<>();
        users.put("id", "001");
        users.put("name", "Chiwa Kantawong");
        users.put("age", 25);

        IndexRequest request = new IndexRequest("users");
        request.id(users.get("id").toString());
        request.source(new ObjectMapper().writeValueAsString(users), XContentType.JSON);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println("response id: " + indexResponse.getId());

        // //

        users = new HashMap<>();
        users.put("id", "002");
        users.put("name", "Pea Kantawong");
        users.put("age", 24);

        request.id(users.get("id").toString());
        request.source(new ObjectMapper().writeValueAsString(users), XContentType.JSON);
        indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println("response id: " + indexResponse.getId());
        return "success";
    }

    @GetMapping(value = "delete")
    public String delete() throws IOException {
        DeleteRequest request = new DeleteRequest("users", "001");
        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
        return deleteResponse.getId();
    }

    //partial update
    @GetMapping(value = "update")
    public UpdateResponse update() throws IOException {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("reason", "daily update");
        UpdateRequest request = new UpdateRequest("users", "001")
                .doc(jsonMap);

        UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
        return updateResponse;
    }

    // //

    @GetMapping(value = "get")
    public GetResponse get() throws IOException {
        GetRequest request = new GetRequest("users", "001");

        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
        return getResponse;
    }

    @GetMapping(value = "getSource")
    public GetSourceResponse getSource() throws IOException {
        GetSourceRequest request = new GetSourceRequest("users", "001");

        GetSourceResponse getResponse = client.getSource(request, RequestOptions.DEFAULT);
        return getResponse;
    }

    @GetMapping(value = "search1")
    public SearchResponse search1() throws IOException {
        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "Chiwa Kantawong");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(matchQueryBuilder);

        SearchRequest searchRequest = new SearchRequest("users");
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }

    @GetMapping(value = "search2")
    public SearchResponse search2() throws IOException {
        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "Chiwa");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(matchQueryBuilder)
                .from(0)
                .size(100)
                .timeout(new TimeValue(3, TimeUnit.MINUTES))
                .sort(new FieldSortBuilder("_id").order(SortOrder.ASC))
                .sort(new ScoreSortBuilder().order(SortOrder.DESC));

        SearchRequest searchRequest = new SearchRequest("users");
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }

    @GetMapping(value = "search3")
    public SearchResponse search3() throws IOException {
        QueryBuilder matchQueryBuilder = QueryBuilders.matchAllQuery();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(matchQueryBuilder);

        SearchRequest searchRequest = new SearchRequest("users");
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }

    @GetMapping(value = "search4")
    public SearchResponse search4() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("name", "Chiwa Kantawong"))
                .from(0)
                .size(100)
                .timeout(new TimeValue(3, TimeUnit.MINUTES))
                .sort(new FieldSortBuilder("_id").order(SortOrder.ASC))
                .sort(new ScoreSortBuilder().order(SortOrder.DESC));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("users");
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }

    @GetMapping(value = "search5")
    public SearchResponse search5() throws IOException {
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("name", "Chiwa Kantawong");
        matchQueryBuilder
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(3)
                .maxExpansions(10);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(matchQueryBuilder);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("users");
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }

    @GetMapping(value = "search6")
    public SearchResponse search6() throws IOException {
        AvgAggregationBuilder aggregation = AggregationBuilders.avg("avg_age").field("age");
        SumAggregationBuilder aggregation2 = AggregationBuilders.sum("sum_age").field("age");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);
        sourceBuilder.aggregation(aggregation2);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("users");
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        return searchResponse;
    }

    @GetMapping(value = "search7")
    public SearchResponse search7() throws IOException {
        AvgAggregationBuilder aggregation = AggregationBuilders.avg("avg_age").field("age");
        SumAggregationBuilder aggregation2 = AggregationBuilders.sum("sum_age").field("age");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("name", "Kantawong"))
                .from(0)
                .size(100)
                .timeout(new TimeValue(3, TimeUnit.MINUTES))
                .sort(new FieldSortBuilder("_id").order(SortOrder.ASC))
                .sort(new ScoreSortBuilder().order(SortOrder.DESC))
                .fetchSource(false);
        sourceBuilder.aggregation(aggregation);
        sourceBuilder.aggregation(aggregation2);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("users");
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }

    @GetMapping(value = "search8")
    public SearchResponse search8() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("name", " Kantawong"))
                .postFilter(QueryBuilders.rangeQuery("age").from(25).to(null)) //gt 24
                .from(0)
                .size(100)
                .timeout(new TimeValue(3, TimeUnit.MINUTES))
                .sort(new FieldSortBuilder("_id").order(SortOrder.ASC))
                .sort(new ScoreSortBuilder().order(SortOrder.DESC));

        SearchRequest searchRequest = new SearchRequest()
                .indices("users")
                .source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }

    @GetMapping(value = "search9")
    public SearchResponse search9() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery())
                .postFilter(QueryBuilders.rangeQuery("age").gte(25))
                .from(0)
                .size(100)
                .timeout(new TimeValue(3, TimeUnit.MINUTES))
                .sort(new FieldSortBuilder("_id").order(SortOrder.ASC))
                .sort(new ScoreSortBuilder().order(SortOrder.DESC));

        SearchRequest searchRequest = new SearchRequest()
                .indices("users")
                .source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }


    @GetMapping(value = "search10")
    public SearchResponse search10() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery())
                .postFilter(QueryBuilders.rangeQuery("age").lt(25))
                .from(0)
                .size(100)
                .timeout(new TimeValue(3, TimeUnit.MINUTES))
                .sort(new FieldSortBuilder("_id").order(SortOrder.ASC))
                .sort(new ScoreSortBuilder().order(SortOrder.DESC));

        SearchRequest searchRequest = new SearchRequest()
                .indices("users")
                .source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }

    @GetMapping(value = "search11")
    public SearchResponse search11() throws IOException {

        MatchQueryBuilder A = QueryBuilders.matchQuery("name", "Chiwa");
        MatchQueryBuilder B = QueryBuilders.matchQuery("name", "Pea");
        MatchQueryBuilder C = QueryBuilders.matchQuery("name", "Kantawong");
        RangeQueryBuilder D = QueryBuilders.rangeQuery("age").lte(25);

        //1.(A AND B AND C)
        BoolQueryBuilder query1 = QueryBuilders.boolQuery();
        query1.must(A).must(B).must(C);

        //2.(A OR B OR C)
        BoolQueryBuilder query2 = QueryBuilders.boolQuery();
        query2.should(A).should(B).should(C);

        //2.(A AND C AND D)
        BoolQueryBuilder query3 = QueryBuilders.boolQuery();
        query3.must(A).must(C).must(D);

        //Compound
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(query2).should(query3);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query3);

        SearchRequest searchRequest = new SearchRequest()
                .indices("users")
                .source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }

    /**
     GET users/_doc/3560500619301

     GET users/_search/
     {
     "query": {
     "match": {
     "name" : "Chiwa "
     }
     }
     }

     GET users/_search/
     {
     "query": {
     "match_all": {
     "_name" : "Chiwa "
     }
     }
     }

     POST /users/_search?size=0
     {
     "query": {
     "match": {
     "name" : "Kantawong"
     }
     },
     "aggs": {
     "avg_age": { "avg": { "field": "age" } },
     "sum_age": { "sum": { "field": "age" } }avg_age": { "avg": { "field": "age" } },
     "sum_age": { "sum": { "field": "age" } },
     "max_age": { "max": { "field": "age" } },
     "min_age": { "min": { "field": "age" } }
     }
     }
     **/


    // ExistsQuery Example
    {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
                .must(QueryBuilders.existsQuery("usuarioIntegracao"))
                .must(QueryBuilders.termsQuery("tabela", "Arquivo", "Mensagem"))
                .must(QueryBuilders.termQuery("statusTexto", "Erro"))
                .must(QueryBuilders.rangeQuery("dataEntrada").from("now-1d/d").timeZone("-03:00"));

        //searchSourceBuilder.query(boolQuery);
    }







    /*
    If you plan to use the _update_by_query API, I'd recommend you to do something like:

POST your_index/_update_by_query
{
  "query": {
    "bool": {
      "must_not": {
        "exists": {
          "field": "tags"
        }
      }
    }
  },
  "script": {
    "source": "ctx._source.tags = ''"
  }
}
Otherwise, just using painless, you can do something like:

{
  "script": {
    "source": """
      if(ctx._source.tags == null) {
        ctx._source.tags = null;
      }
    """
  }
}
     */

    /*
    Using Java UpdateByQueryRequest
https://www.fatalerrors.org/a/elasticsearch-java-high-level-rest-client-update-by-query-api.html
     */
    @GetMapping(value = "search12")
    public BulkByScrollResponse search120() throws IOException {

        UpdateByQueryRequest request = new UpdateByQueryRequest("users");
        request.setQuery(new MatchQueryBuilder("_id", "001"));

        request.setScript(
                new Script(
                        ScriptType.INLINE, "painless",
                        "ctx._source.full_name = 'Chiwa Kantawong updated by Java'",
                        Collections.emptyMap()));
        request.setRefresh(true);

        BulkByScrollResponse bulkResponse = client.updateByQuery(request, RequestOptions.DEFAULT);
        return bulkResponse;
    }

    @GetMapping(value = "search13")
    public BulkByScrollResponse search130() throws IOException {

        UpdateByQueryRequest request = new UpdateByQueryRequest("users");
        request.setQuery(new MatchQueryBuilder("_id", "001"));

        Map<String, Object> params = new HashMap<>();
        params.put("my_text", ".....Hello world");

        request.setScript(
                new Script(
                        ScriptType.INLINE, "painless",
                        "ctx._source.full_name = 'Chiwa Kantawong ' + params.my_text",
                        params));

        request.setRefresh(true);

        BulkByScrollResponse bulkResponse = client.updateByQuery(request, RequestOptions.DEFAULT);
        return bulkResponse;
    }


    // Asynchronous execution
    {
        UpdateByQueryRequest request = new UpdateByQueryRequest("users");

        ActionListener listener = new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkResponse) {
            }

            @Override
            public void onFailure(Exception e) {
            }
        };
        client.updateByQueryAsync(request, RequestOptions.DEFAULT, listener);
    }

}