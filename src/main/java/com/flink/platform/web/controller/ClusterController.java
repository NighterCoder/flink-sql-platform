package com.flink.platform.web.controller;

import com.flink.platform.web.common.entity.DtoCluster;
import com.flink.platform.web.common.entity.Msg;
import com.flink.platform.web.common.entity.entity2table.Cluster;
import com.flink.platform.web.service.ClusterService;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/cluster")
public class ClusterController extends BaseController{

    @Autowired
    private ClusterService clusterService;

    @RequestMapping(value = "add",method = RequestMethod.POST)
    public Msg save(@RequestBody DtoCluster dtoCluster){
        Cluster cluster = new Cluster();
        BeanUtils.copyProperties(dtoCluster,cluster);
        clusterService.save(cluster);
        return success();
    }


    @RequestMapping(value = "/list",method = RequestMethod.GET)
    public Msg all(){
        List<DtoCluster> dtoClusters = clusterService.list(null).stream().map(item->{
            DtoCluster dtoCluster = new DtoCluster();
            BeanUtils.copyProperties(item,dtoCluster);
            return dtoCluster;
        }).collect(Collectors.toList());
        return success(dtoClusters);

    }





}
