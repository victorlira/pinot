package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.StarTreeQueryImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.linkedin.thirdeye.util.ThirdEyeUriUtils;
import com.sun.jersey.api.NotFoundException;

import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Path("/timeSeries")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeTimeSeriesResource
{
  private final StarTreeManager manager;

  public ThirdEyeTimeSeriesResource(StarTreeManager manager)
  {
    this.manager = manager;
  }

  @GET
  @Path("/{collection}/{metric}/{start}/{end}")
  @Timed
  public List<Result> getTimeSeries(@PathParam("collection") String collection,
                                    @PathParam("metric") String metric,
                                    @PathParam("start") Long start,
                                    @PathParam("end") Long end,
                                    @Context UriInfo uriInfo)
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }
    if (!starTree.getConfig().getMetricNames().contains(metric))
    {
      throw new NotFoundException("No metric " + metric + " in collection " + collection);
    }

    // Expand queries
    List<StarTreeQuery> queries
            = StarTreeUtils.expandQueries(starTree,
                                          ThirdEyeUriUtils.createQueryBuilder(starTree, uriInfo)
                                                          .setTimeRange(start, end).build());

    // Filter queries
    queries = StarTreeUtils.filterQueries(queries, uriInfo.getQueryParameters());

    // Query tree for time series
    List<Result> results = new ArrayList<Result>(queries.size());
    for (StarTreeQuery query : queries)
    {
      List<StarTreeRecord> timeSeries = starTree.getTimeSeries(query);

      Result result = new Result();
      result.setMetricName(metric);
      result.setTimeSeries(convertTimeSeries(metric, timeSeries));
      result.setDimensionValues(query.getDimensionValues());
      results.add(result);
    }

    return results;
  }

  private static List<List<Long>> convertTimeSeries(String metric, List<StarTreeRecord> records)
  {
    List<List<Long>> timeSeries = new ArrayList<List<Long>>(records.size());

    for (StarTreeRecord record : records)
    {
      timeSeries.add(Arrays.asList(record.getTime(), record.getMetricValues().get(metric).longValue()));
    }

    return timeSeries;
  }

  public static class Result
  {
    @NotNull
    private String metricName;

    @NotNull
    private Map<String, String> dimensionValues;

    @NotNull
    private List<List<Long>> timeSeries;

    @JsonProperty
    public String getMetricName()
    {
      return metricName;
    }

    @JsonProperty
    public void setMetricName(String metricName)
    {
      this.metricName = metricName;
    }

    @JsonProperty
    public Map<String, String> getDimensionValues()
    {
      return dimensionValues;
    }

    @JsonProperty
    public void setDimensionValues(Map<String, String> dimensionValues)
    {
      this.dimensionValues = dimensionValues;
    }

    @JsonProperty
    public List<List<Long>> getTimeSeries()
    {
      return timeSeries;
    }

    @JsonProperty
    public void setTimeSeries(List<List<Long>> timeSeries)
    {
      this.timeSeries = timeSeries;
    }
  }
}