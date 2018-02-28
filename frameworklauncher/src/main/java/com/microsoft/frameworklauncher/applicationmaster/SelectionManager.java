// Copyright (c) Microsoft Corporation
// All rights reserved. 
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated 
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation 
// the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and 
// to permit persons to whom the Software is furnished to do so, subject to the following conditions:
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING 
// BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, 
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. 


package com.microsoft.frameworklauncher.applicationmaster;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.frameworklauncher.common.exceptions.NotAvailableException;
import com.microsoft.frameworklauncher.common.exts.CommonExts;
import com.microsoft.frameworklauncher.common.log.DefaultLogger;
import com.microsoft.frameworklauncher.common.model.ClusterConfiguration;
import com.microsoft.frameworklauncher.common.model.NodeConfiguration;
import com.microsoft.frameworklauncher.common.utils.HadoopUtils;
import com.microsoft.frameworklauncher.common.utils.RangeUtils;
import com.microsoft.frameworklauncher.common.utils.YamlUtils;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import com.microsoft.frameworklauncher.common.model.*;

import java.util.*;

/**
 * Based on:
 * RM's all running nodes' status {@link NodeReport}
 * AM's all outstanding requested container requests {@link ContainerRequest}
 * Given:
 * A Task's raw request {@link ResourceDescriptor}
 * Provides:
 * The {@link SelectionResult} which helps to construct the {@link ContainerRequest}
 * for the Task to request container.
 */
public class SelectionManager { // THREAD SAFE
  private static final DefaultLogger LOGGER = new DefaultLogger(SelectionManager.class);

  private final ApplicationMaster am;
  private final LinkedHashMap<String, Node> allNodes = new LinkedHashMap<>();
  private final LinkedHashMap<String, ResourceDescriptor> localTriedResource = new LinkedHashMap<>();
  private final List<String> filteredNodes = new ArrayList<String>();

  public SelectionManager(ApplicationMaster am) {
    this.am = am;
  }

  public synchronized void addNode(NodeReport nodeReport) throws Exception {
    addNode(Node.fromNodeReport(nodeReport));
  }

  private void randomizeNodes() {

    filteredNodes.clear();
    for (String nodeName : allNodes.keySet()) {
      filteredNodes.add(nodeName);
    }
    int randomTimes = filteredNodes.size();
    while (randomTimes-- > 0) {
      int randomIndex = Math.abs(new Random().nextInt(filteredNodes.size()));
      filteredNodes.add(filteredNodes.remove(randomIndex));
    }
  }

  private void filterNodesByLabel(String requestNodeLabel) {
    if (requestNodeLabel != null) {
      for (int i = filteredNodes.size(); i > 0; i--) {
        Set<String> availableNodeLabels = allNodes.get(filteredNodes.get(i - 1)).getLabels();
        if (!HadoopUtils.matchNodeLabel(requestNodeLabel, availableNodeLabels)) {
          LOGGER.logDebug("NodeLabel does not match: Node: [%s] Request NodeLabel: [%s]",
              filteredNodes.get(i - 1), requestNodeLabel);
          filteredNodes.remove(i - 1);
        }
      }
    }
  }

  private void filterNodesByGpuType(String requestNodeGpuType) {
    if (requestNodeGpuType != null) {
      Map<String, NodeConfiguration> configuredNodes = am.getClusterConfiguration().getNodes();
      if (configuredNodes != null) {
        for (int i = filteredNodes.size(); i > 0; i--) {
          String nodeHost = filteredNodes.get(i - 1);
          if (!configuredNodes.containsKey(nodeHost)) {
            LOGGER.logDebug("Node:[%s] is not found in clusterConfiguration: Request NodeGpuType: [%s]", nodeHost, requestNodeGpuType);
            filteredNodes.remove(i - 1);
            continue;
          }
          List<String> requestNodeGpuTypes = Arrays.asList(requestNodeGpuType.split(","));
          String availableNodeGpuType = configuredNodes.get(nodeHost).getGpuType();
          if (!requestNodeGpuTypes.contains(availableNodeGpuType)) {
            LOGGER.logDebug("NodeGpuType does not match: Node: [%s] Request NodeGpuType: [%s], Available NodeGpuType: [%s]",
                nodeHost, requestNodeGpuType, availableNodeGpuType);
            filteredNodes.remove(i - 1);
            continue;
          }
        }
      }
    }
  }

  private void filterNodesForNonGpuTask(ResourceDescriptor requestResource) {
    if (requestResource != null && requestResource.getGpuNumber() == 0) {
      for (int i = filteredNodes.size(); i > 0; i--) {
        Node node = allNodes.get(filteredNodes.get(i - 1));
        ResourceDescriptor totalResource = node.getTotalResource();
        if (totalResource.getGpuNumber() > 0) {
          LOGGER.logDebug("skip nodes with Gpu resource for non-gpu task: Node [%s], Request Resource: [%s], Total Resource: [%s]",
              node.getHost(), requestResource, totalResource);
          filteredNodes.remove(i - 1);
        }
      }
    }
  }

  private void filterNodesByResource(ResourceDescriptor requestResource, Boolean skipLocalTriedResource) {
    if (requestResource != null) {
      for (int i = filteredNodes.size(); i > 0; i--) {
        Node node = allNodes.get(filteredNodes.get(i - 1));
        ResourceDescriptor availableResource = node.getAvailableResource();
        if (skipLocalTriedResource && localTriedResource.containsKey(node.getHost())) {
          ResourceDescriptor.subtractFrom(availableResource, localTriedResource.get(node.getHost()));
        }
        if (!ResourceDescriptor.fitsIn(requestResource, availableResource)) {
          LOGGER.logDebug("Resource does not fit in: Node: [%s] Request Resource: [%s], Available Resource: [%s]",
              node.getHost(), requestResource, availableResource);
          filteredNodes.remove(i - 1);
        }
      }
    }
  }

  private void filterNodesByGroupSelectionPolicy(ResourceDescriptor requestResource, int pendingTaskNumber) {
  //TODO: Node GPU policy filter the nodes;
  }

  private SelectionResult SelectNodes(ResourceDescriptor requestResource, int pendingTaskNumber) {

    String nodeSelectionPolicy = am.getConfiguration().getLauncherConfig().getAmNodeSelectionPolicy();

    if(nodeSelectionPolicy.equals(SelectionPolicy.PACKING.toString())) {
      return selectNodesByPacking(requestResource, pendingTaskNumber);
    } else if(nodeSelectionPolicy.equals(SelectionPolicy.COHOST.toString())) {
      SelectionResult result = selectNodesByCoHost(requestResource, pendingTaskNumber);
      if(result.getSelectedNodeHosts().size() > 0) {
        return result;
      }
    } else if(nodeSelectionPolicy.equals(SelectionPolicy.TOPOLOGY.toString())) {
      //TODO: schedule task to node which has best gpu locality
    }
    // Use packing as default node selection strategy.
    return selectNodesByPacking(requestResource, pendingTaskNumber);
  }

  //Default Node Selection strategy.
  private SelectionResult selectNodesByCoHost(ResourceDescriptor requestResource, int pendingTaskNumber) {

    int requestNumber = pendingTaskNumber * am.getConfiguration().getLauncherConfig().getAmSearchNodeBufferFactor();
    List<String> allocatedHosts = am.getStatusManager().getApplicationAllocatedHosts();
    List<Node> candidateNodes = new ArrayList<Node>();
    SelectionResult result = new SelectionResult();
    for (String nodeName : filteredNodes) {
      if(allocatedHosts.contains(nodeName)) {
        candidateNodes.add(allNodes.get(nodeName));
      }
    }
    Collections.sort(candidateNodes);
    for (int i = 0; i < requestNumber && i < candidateNodes.size(); i++) {
      Node select = candidateNodes.get(i);
      Long gpuAttribute = requestResource.getGpuAttribute();
      if (gpuAttribute == 0) {
        gpuAttribute = selectCandidateGpuAttribute(select, requestResource.getGpuNumber());
      }
      result.addSelection(select.getHost(), gpuAttribute, select.getAvailableResource().getPortRanges());
    }
    return result;
  }

  //Default Node Selection strategy.
  private SelectionResult selectNodesByPacking(ResourceDescriptor requestResource, int pendingTaskNumber) {

    int requestNumber = pendingTaskNumber * am.getConfiguration().getLauncherConfig().getAmSearchNodeBufferFactor();
    List<Node> candidateNodes = new ArrayList<Node>();
    SelectionResult result = new SelectionResult();
    for (String nodeName : filteredNodes) {
      candidateNodes.add(allNodes.get(nodeName));
    }
    Collections.sort(candidateNodes);
    for (int i = 0; i < requestNumber && i < candidateNodes.size(); i++) {
      Node select = candidateNodes.get(i);
      Long gpuAttribute = requestResource.getGpuAttribute();
      if (gpuAttribute == 0) {
        gpuAttribute = selectCandidateGpuAttribute(select, requestResource.getGpuNumber());
      }
      result.addSelection(select.getHost(), gpuAttribute, select.getAvailableResource().getPortRanges());
    }
    return result;
  }


  @VisibleForTesting
  public synchronized void addNode(Node reportedNode) {
    if (!allNodes.containsKey(reportedNode.getHost())) {
      LOGGER.logDebug("addNode: %s", reportedNode);
      allNodes.put(reportedNode.getHost(), reportedNode);
    } else {
      Node existNode = allNodes.get(reportedNode.getHost());
      existNode.updateFromReportedNode(reportedNode);
      LOGGER.logDebug("addNode: %s ", existNode);
    }
  }

  public synchronized void removeNode(NodeReport nodeReport) throws Exception {
    removeNode(Node.fromNodeReport(nodeReport));
  }

  @VisibleForTesting
  public synchronized void removeNode(Node reportedNode) {
    if (allNodes.containsKey(reportedNode.getHost())) {
      LOGGER.logDebug("removeNode: %s", reportedNode);
      allNodes.remove(reportedNode.getHost());
    }
  }

  // Add outstanding requested container request
  public synchronized void addContainerRequest(ContainerRequest request) throws Exception {
    addContainerRequest(
        ResourceDescriptor.fromResource(request.getCapability()),
        request.getNodes());
  }

  @VisibleForTesting
  public synchronized void addContainerRequest(ResourceDescriptor resource, List<String> nodeHosts) {
    for (String nodeHost : nodeHosts) {
      if (allNodes.containsKey(nodeHost)) {
        allNodes.get(nodeHost).addContainerRequest(resource);
        if (!localTriedResource.containsKey(nodeHost)) {
          localTriedResource.put(nodeHost, YamlUtils.deepCopy(resource, ResourceDescriptor.class));
        } else {
          ResourceDescriptor triedResource = localTriedResource.get(nodeHost);
          ResourceDescriptor.addTo(triedResource, resource);
        }
      } else {
        LOGGER.logWarning("addContainerRequest: Node is no longer a candidate: %s", nodeHost);
      }
    }
  }

  // Remove outstanding requested container request
  public synchronized void removeContainerRequest(ContainerRequest request) throws Exception {
    removeContainerRequest(
        ResourceDescriptor.fromResource(request.getCapability()),
        request.getNodes());
  }

  @VisibleForTesting
  public synchronized void removeContainerRequest(ResourceDescriptor resource, List<String> nodeHosts) {
    for (String nodeHost : nodeHosts) {
      if (allNodes.containsKey(nodeHost)) {
        allNodes.get(nodeHost).removeContainerRequest(resource);
      } else {
        LOGGER.logWarning("removeContainerRequest: Node is no longer a candidate: %s", nodeHost);
      }
    }
  }

  @VisibleForTesting
  public synchronized SelectionResult select(ResourceDescriptor requestResource, String taskRoleName) throws NotAvailableException {
    String requestNodeLabel = am.getRequestManager().getTaskPlatParams().get(taskRoleName).getTaskNodeLabel();
    String requestNodeGpuType = am.getRequestManager().getTaskPlatParams().get(taskRoleName).getTaskNodeGpuType();
    int pendingTaskNumber = am.getStatusManager().getUnAllocatedTaskCount(taskRoleName);
    List<Range> allocatedPort =am.getStatusManager().getAllocatedTaskPorts(taskRoleName);
    return select(requestResource, requestNodeLabel, requestNodeGpuType, pendingTaskNumber, allocatedPort);
  }

  @VisibleForTesting
  public synchronized SelectionResult select(ResourceDescriptor requestResource,  String requestNodeLabel, String requestNodeGpuType, int pendingTaskNumber) throws NotAvailableException {
    return select(requestResource, requestNodeLabel, requestNodeGpuType, pendingTaskNumber, null);
  }

  public synchronized SelectionResult select(ResourceDescriptor requestResource,  String requestNodeLabel, String requestNodeGpuType, int pendingTaskNumber, List<Range> allocatedPort) throws NotAvailableException {

    LOGGER.logInfo(
        "select: Request: Resource: [%s], NodeLabel: [%s], NodeGpuType: [%s], TaskNumber: [%d]",
        requestResource, requestNodeLabel, requestNodeGpuType, pendingTaskNumber);

    randomizeNodes();
    if (am.getConfiguration().getLauncherConfig().getAmEnableNodeLabelFilter()) {
      filterNodesByLabel(requestNodeLabel);
    }
    if (am.getConfiguration().getLauncherConfig().getAmEnableGpuTypeFilter()) {
      filterNodesByGpuType(requestNodeGpuType);
    }

    if (!am.getConfiguration().getLauncherConfig().getAmAllowNonGpuTaskOnGpuNode()) {
      filterNodesForNonGpuTask(requestResource);
    }

    ResourceDescriptor optimizedRequestResource = YamlUtils.deepCopy(requestResource, ResourceDescriptor.class);
    if (am.getConfiguration().getLauncherConfig().getAmAllTaskWithTheSamePorts()) {
      if (RangeUtils.getValueNumber(allocatedPort) > 0) {
        optimizedRequestResource.setPortRanges(allocatedPort);
      }
    }

    filterNodesByResource(optimizedRequestResource, am.getConfiguration().getLauncherConfig().getAmSkipLocalTriedResource());

    filterNodesByGroupSelectionPolicy(optimizedRequestResource, pendingTaskNumber);
    if (filteredNodes.size() < pendingTaskNumber) {
      //don't have candidate nodes for this request.
      if (requestNodeGpuType != null || requestResource.getPortNumber() > 0) {
        //If gpuType or portNumber is specified, abort this request and try later.
        throw new NotAvailableException(String.format("Don't have enough nodes to fix in optimizedRequestResource:%s, NodeGpuType: [%s]",
            optimizedRequestResource, requestNodeGpuType));
      }
    }

    SelectionResult selectionResult = SelectNodes(optimizedRequestResource, pendingTaskNumber);

    // after find a set of candidates, if the port was not allocated or specified previously, need allocate the port for this request.
    if (RangeUtils.getValueNumber(optimizedRequestResource.getPortRanges()) <= 0 && optimizedRequestResource.getPortNumber() > 0) {
      List<Range> newCandidatePorts = RangeUtils.getSubRange(selectionResult.getOverlapPorts(), optimizedRequestResource.getPortNumber(),
          am.getConfiguration().getLauncherConfig().getAmContainerBasePort());

      if (RangeUtils.getValueNumber(newCandidatePorts) >= optimizedRequestResource.getPortNumber()) {
        optimizedRequestResource.setPortRanges(newCandidatePorts);
        LOGGER.logDebug("Allocated port: optimizedRequestResource: [%s]", optimizedRequestResource);
      } else {
        throw new NotAvailableException("The selected candidate nodes don't have enough ports");
      }
    }
    selectionResult.setOptimizedResource(optimizedRequestResource);

    return selectionResult;
  }

  @VisibleForTesting
  public synchronized Long selectCandidateGpuAttribute(Node node, Integer requestGpuNumber) {
    ResourceDescriptor nodeAvailable = node.getAvailableResource();
    assert (requestGpuNumber <= nodeAvailable.getGpuNumber());

    Long selectedGpuAttribute = 0L;
    Long availableGpuAttribute = nodeAvailable.getGpuAttribute();

    // By default, using the simple sequential selection.
    // To improve it, considers the Gpu topology structure, find a node which can minimize
    // the communication cost among Gpus;
    for (int i = 0; i < requestGpuNumber; i++) {
      selectedGpuAttribute += (availableGpuAttribute - (availableGpuAttribute & (availableGpuAttribute - 1)));
      availableGpuAttribute &= (availableGpuAttribute - 1);
    }
    return selectedGpuAttribute;
  }
}
