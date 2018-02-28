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

package com.microsoft.frameworklauncher.common.utils;

import com.microsoft.frameworklauncher.common.model.*;

import java.net.PortUnreachableException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Random;

public class RangeUtils {

  /*
    sort the list range from small to big.
   */
  public static List<Range> SortRangeList(List<Range> ranges) {
    List<Range> newList = cloneList(ranges);
    Collections.sort(newList);
    return newList;
  }

  /*
    count the value number in a  range list.
   */
  public static int getValueNumber(List<Range> rangeList) {
    if (rangeList == null || rangeList.size() == 0) {
      return 0;
    }

    List<Range> newRangeList = coalesceRangeList(rangeList);
    int valueCount = 0;
    for (Range range : newRangeList) {
      valueCount += (range.getEnd() - range.getBegin() + 1);
    }
    return valueCount;
  }

  /*
    coalesce the duplicate or overlap range in the range list.
   */
  public static List<Range> coalesceRangeList(List<Range> rangeList) {
    if (rangeList == null || rangeList.isEmpty()) {
      return rangeList;
    }

    List<Range> sortedList = SortRangeList(rangeList);
    List<Range> resultList = new ArrayList<Range>();

    Range current = sortedList.get(0).clone();
    resultList.add(current);

    for (Range range : sortedList) {
      // Skip if this range is equivalent to the current range.
      if (range.getBegin().intValue() == current.getBegin().intValue()
          && range.getEnd().intValue() == current.getEnd().intValue()) {
        continue;
      }
      // If the current range just needs to be extended on the right.
      if (range.getBegin().intValue() == current.getBegin().intValue()
          && range.getEnd() > current.getEnd()) {
        current.setEnd(range.getEnd());
      } else if (range.getBegin() > current.getBegin()) {
        // If we are starting farther ahead, then there are 2 cases:
        if (range.getBegin() <= current.getEnd() + 1) {
          // 1. Ranges are overlapping and we can merge them.
          current.setEnd(Math.max(current.getEnd(), range.getEnd()));
        } else {
          // 2. No overlap and we are adding a new range.
          current = range.clone();
          resultList.add(current);
        }
      }
    }
    return resultList;
  }

  /*
    get the overlap part of tow range lists
   */
  public static List<Range> intersectRangeList(List<Range> leftRange, List<Range> rightRange) {

    if (leftRange == null || rightRange == null) {
      return null;
    }

    List<Range> leftList = coalesceRangeList(leftRange);
    List<Range> rightList = coalesceRangeList(rightRange);

    List<Range> result = new ArrayList<Range>();
    int i = 0;
    int j = 0;
    while (i < leftList.size() && j < rightList.size()) {
      Range left = leftList.get(i);
      Range right = rightList.get(j);
      // 1. no overlap, right is bigger than left
      if (left.getEnd() < right.getBegin()) {
        i++;
        // 2. no overlap, left is bigger than right
      } else if (right.getEnd() < left.getBegin()) {
        j++;
        // 3. has overlap, get the overlap
      } else {
        result.add(Range.newInstance(Math.max(left.getBegin(), right.getBegin()), Math.min(left.getEnd(), right.getEnd())));
        if (left.getEnd() < right.getEnd()) {
          i++;
        } else {
          j++;
        }
      }
    }
    return result;
  }

  /*
    delete the overlap part from leftRange.
   */
  public static List<Range> subtractRange(List<Range> leftRange, List<Range> rightRange) {

    if (leftRange == null || rightRange == null) {
      return leftRange;
    }

    List<Range> result = coalesceRangeList(leftRange);
    List<Range> rightList = coalesceRangeList(rightRange);

    int i = 0;
    int j = 0;
    while (i < result.size() && j < rightList.size()) {
      Range left = result.get(i);
      Range right = rightList.get(j);
      // 1. no overlap, right is bigger than left
      if (left.getEnd() < right.getBegin()) {
        i++;
        // 2. no overlap, left is bigger than right
      } else if (right.getEnd() < left.getBegin()) {
        j++;
        // 3. has overlap, left is less than right
      } else {
        if (left.getBegin() < right.getBegin()) {
          //3.1 Left start early than right, cut at the right begin;
          if (left.getEnd() <= right.getEnd()) {
            //3.1.1 Left end early than right, do nothing try next left;
            left.setEnd(right.getBegin() - 1);
            i++;
          } else {
            //3.1.2 Left end later than right, create a new range in left;
            Range newRange = Range.newInstance(right.getEnd() + 1, left.getEnd());
            result.add(i + 1, newRange);
            left.setEnd(right.getBegin() - 1);
            j++;
          }
        } else {
          // 3.2 left start later than right
          if (left.getEnd() <= right.getEnd()) {
            //3.2.1 left end early than right, just remove left
            result.remove(i);
          } else {
            //3.2.2 left end later than right, just remove left
            left.setBegin(right.getEnd() + 1);
            j++;
          }
        }
      }
    }
    return result;
  }

  /*
    add rightRange to leftRange, will ingore the overlap range.
   */
  public static List<Range> addRange(List<Range> leftRange, List<Range> rightRange) {

    if (leftRange == null)
      return rightRange;
    if (rightRange == null)
      return leftRange;

    List<Range> result = coalesceRangeList(leftRange);
    result.addAll(rightRange);
    return coalesceRangeList(result);
  }

  /*
    verify if the bigRange include the small range
   */
  public static boolean fitInRange(List<Range> smallRange, List<Range> bigRange) {
    if (smallRange == null) {
      return true;
    }

    if (bigRange == null) {
      return false;
    }

    List<Range> result = coalesceRangeList(bigRange);
    List<Range> smallRangeList = coalesceRangeList(smallRange);
    int i = 0;
    int j = 0;
    while (i < result.size() && j < smallRangeList.size()) {
      Range big = result.get(i);
      Range small = smallRangeList.get(j);

      if (small.getBegin() < big.getBegin()) {
        return false;
      }

      if (small.getBegin() <= big.getEnd()) {
        if (small.getEnd() > big.getEnd()) {
          return false;
        } else {
          big.setBegin(small.getEnd() + 1);
          j++;
        }
      } else {
        i++;
      }
    }
    return (j >= smallRangeList.size());
  }

  /*
    get a Random subRange list from the available range list, all the value in the subRange is bigger than baseValue.
    and the value count in the subRange is requestNumber
   */
  public static List<Range> getSubRange(List<Range> availableRange, int requestNumber, int baseValue) {

    List<Range> resultList = new ArrayList<Range>();
    Random random = new Random();
    //Pick a random number from 0 to the max value;
    int maxValue = availableRange.get(availableRange.size() - 1).getEnd();
    int randomBase = random.nextInt(maxValue) + 1;

    // try different randomBase to find enough request number. If still cannot find enough request
    // number when randomBase reduce to 0, return null.
    while (randomBase > 0) {
      resultList.clear();
      int needNumber = requestNumber;
      randomBase = randomBase / 2;
      int newbaseValue = baseValue + randomBase;
      for (Range range : availableRange) {
        if (range.getEnd() < newbaseValue) {
          continue;
        }
        int start = Math.max(range.getBegin(), newbaseValue);
        if ((range.getEnd() - start + 1) >= needNumber) {
          resultList.add(Range.newInstance(start, start + needNumber - 1));
          return resultList;
        } else {
          resultList.add(Range.newInstance(start, range.getEnd()));
          needNumber -= (range.getEnd() - start + 1);
        }
      }
    }
    return null;
  }

  public static boolean isEqualRangeList(List<Range> leftRangeList, List<Range> rightRangeList) {
    List<Range> leftRange = coalesceRangeList(leftRangeList);
    List<Range> rightRange = coalesceRangeList(rightRangeList);

    if(leftRange == null || rightRange == null) {
      if(leftRange == rightRange) {
        return true;
      }else {
        return false;
      }
    }
    if(leftRange.size() != rightRange.size()) {
      return false;
    }
    for(int i = 0; i < leftRange.size(); i++) {
      if(leftRange.get(i).getBegin() != rightRange.get(i).getBegin()) {
        return false;
      }
      if(leftRange.get(i).getEnd() != rightRange.get(i).getEnd()) {
        return false;
      }
    }
    return true;
  }

  public static List<Range> cloneList(List<Range> list) {
    List<Range> newList = new ArrayList<Range>();
    for (Range range : list) {
      newList.add(range.clone());
    }
    return newList;
  }

  /*
    get the value at "index" location in the Range list
   */
  public static Integer getValue(List<Range> list, int index) {
    if (list == null) {
      return -1;
    }
    List<Range> ranges = coalesceRangeList(list);
    int i = index;
    for (Range range : ranges) {
      if (range.getEnd() - range.getBegin() < i) {
        i -= (range.getEnd() - range.getBegin() + 1);
      } else {
        return (range.getBegin() + i);
      }
    }
    return -1;
  }
}
