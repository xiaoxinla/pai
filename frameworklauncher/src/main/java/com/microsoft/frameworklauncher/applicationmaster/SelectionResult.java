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

import com.microsoft.frameworklauncher.common.exts.CommonExts;
import com.microsoft.frameworklauncher.common.model.Range;
import com.microsoft.frameworklauncher.common.model.ResourceDescriptor;
import com.microsoft.frameworklauncher.common.utils.RangeUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class SelectionResult {

  private Map<String, Long> selectedNodes = new HashMap<String, Long>();
  private List<Range> overlapPorts = new ArrayList<>();
  private ResourceDescriptor optimizedResource = ResourceDescriptor.newInstance(0, 0, 0, (long) 0);

  public void addSelection(String hostName, Long gpuAttribute, List<Range> portList) {
    if (selectedNodes.isEmpty()) {
      selectedNodes.put(hostName, gpuAttribute);
      overlapPorts = RangeUtils.coalesceRangeList(portList);
      return;
    }
    if (selectedNodes.containsKey(hostName)) {
      selectedNodes.remove(hostName);
    }
    selectedNodes.put(hostName, gpuAttribute);
    overlapPorts = RangeUtils.intersectRangeList(overlapPorts, portList);
  }

  public List<Range> getOverlapPorts() {
    return overlapPorts;
  }

  public List<String> getSelectedNodeHosts() {
    List<String> hostList = new ArrayList<String>();
    for (String hostName : selectedNodes.keySet()) {
      hostList.add(hostName);

    }
    return hostList;
  }

  public Long getGpuAttribute(String hostName) {
    return selectedNodes.get(hostName);
  }

  public void setOptimizedResource(ResourceDescriptor optimizedResource) {
    this.optimizedResource = optimizedResource;
  }

  public ResourceDescriptor getOptimizedResource() {
    return optimizedResource;
  }

  @Override
  public String toString() {
    String output = "SelectionResult:";
    for (Map.Entry<String, Long> entry : selectedNodes.entrySet()) {
      output += String.format(" [Host: %s GpuAttribute: %s]", entry.getKey(), CommonExts.toStringWithBits(entry.getValue()));
    }
    return output;
  }
}
