/*
 * Copyright 2011 Happy-Camper Street.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package st.happy_camper.hadoop.scala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.RawComparator
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.Partitioner
import org.apache.hadoop.mapreduce.Reducer

/**
 * @author ueshin
 */
private class Job(private val job: mapreduce.Job) {

  /**
   * @param dir
   */
  def workingDirectory_=(dir: Path) {
    job.setWorkingDirectory(dir)
  }

  /**
   * @return
   */
  def workingDirectory = job.getWorkingDirectory

  /**
   * @param dir
   */
  def workingDirectory(dir: Path): Job = {
    workingDirectory = dir
    this
  }

  /**
   * @param cls
   */
  def inputFormat_=[KEY <: Writable: Manifest, VALUE <: Writable: Manifest](cls: Class[_ <: InputFormat[KEY, VALUE]]) {
    job.setInputFormatClass(cls)
  }

  /**
   * @return
   */
  def inputFormat = job.getInputFormatClass

  /**
   * @param cls
   * @return
   */
  def inputFormat[KEY <: Writable: Manifest, VALUE <: Writable: Manifest](cls: Class[_ <: InputFormat[KEY, VALUE]]): Job = {
    inputFormat = cls
    this
  }

  /**
   * @param cls
   */
  def mapper_=[KEYIN <: Writable: Manifest, VALUEIN <: Writable: Manifest, KEYOUT <: Writable: Manifest, VALUEOUT <: Writable: Manifest](cls: Class[_ <: Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]]) {
    job.setMapperClass(cls)
    mapOutputKey = manifest[KEYOUT].erasure.asInstanceOf[Class[KEYOUT]]
    mapOutputValue = manifest[VALUEOUT].erasure.asInstanceOf[Class[VALUEOUT]]
  }

  /**
   * @return
   */
  def mapper = job.getMapperClass

  /**
   * @param cls
   */
  def mapper[KEYIN <: Writable: Manifest, VALUEIN <: Writable: Manifest, KEYOUT <: Writable: Manifest, VALUEOUT <: Writable: Manifest](cls: Class[_ <: Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]]): Job = {
    mapper = cls
    this
  }

  /**
   * @param cls
   */
  def mapOutputKey_=(cls: Class[_ <: Writable]) {
    job.setMapOutputKeyClass(cls)
  }

  /**
   * @return
   */
  def mapOutputKey = job.getMapOutputKeyClass

  /**
   * @param cls
   * @return
   */
  def mapOutputKey(cls: Class[_ <: Writable]): Job = {
    mapOutputKey = cls
    this
  }

  /**
   * @param cls
   */
  def mapOutputValue_=(cls: Class[_ <: Writable]) {
    job.setMapOutputValueClass(cls)
  }

  /**
   * @return
   */
  def mapOutputValue = job.getOutputValueClass

  /**
   * @param cls
   */
  def mapOutputValue(cls: Class[_ <: Writable]): Job = {
    mapOutputValue = cls
    this
  }

  /**
   * @param cls
   */
  def partitioner_=[KEY <: Writable: Manifest, VALUE <: Writable: Manifest](cls: Class[_ <: Partitioner[KEY, VALUE]]) {
    job.setPartitionerClass(cls)
    mapOutputKey = manifest[KEY].erasure.asInstanceOf[Class[KEY]]
    mapOutputValue = manifest[VALUE].erasure.asInstanceOf[Class[VALUE]]
  }

  /**
   * @return
   */
  def partitioner = job.getPartitionerClass

  /**
   * @param cls
   * @return
   */
  def partitioner[KEY <: Writable: Manifest, VALUE <: Writable: Manifest](cls: Class[_ <: Partitioner[KEY, VALUE]]): Job = {
    partitioner = cls
    this
  }

  /**
   * @param cls
   */
  def sortComparator_=[T <: Writable: Manifest](cls: Class[_ <: RawComparator[T]]) {
    job.setSortComparatorClass(cls)
    mapOutputKey = manifest[T].erasure.asInstanceOf[Class[T]]
  }

  /**
   * @return
   */
  def sortComparator = job.getSortComparator

  /**
   * @param cls
   * @return
   */
  def sortComparator[T <: Writable: Manifest](cls: Class[_ <: RawComparator[T]]): Job = {
    sortComparator = cls
    this
  }

  /**
   * @param cls
   */
  def combiner_=[KEY <: Writable: Manifest, VALUE <: Writable: Manifest](cls: Class[_ <: Reducer[KEY, VALUE, KEY, VALUE]]) {
    job.setCombinerClass(cls)
    mapOutputKey = manifest[KEY].erasure.asInstanceOf[Class[KEY]]
    mapOutputValue = manifest[VALUE].erasure.asInstanceOf[Class[VALUE]]
  }

  /**
   * @return
   */
  def combiner = job.getCombinerClass

  /**
   * @param cls
   * @return
   */
  def combiner[KEY <: Writable: Manifest, VALUE <: Writable: Manifest](cls: Class[_ <: Reducer[KEY, VALUE, KEY, VALUE]]): Job = {
    combiner = cls
    this
  }

  /**
   * @param cls
   */
  def groupingComparator_=[T <: Writable: Manifest](cls: Class[_ <: RawComparator[T]]) {
    job.setGroupingComparatorClass(cls)
    mapOutputKey = manifest[T].erasure.asInstanceOf[Class[T]]
  }

  /**
   * @return
   */
  def groupingComparator = job.getGroupingComparator

  /**
   * @param cls
   * @return
   */
  def groupingComparator[T <: Writable: Manifest](cls: Class[_ <: RawComparator[T]]): Job = {
    groupingComparator = cls
    this
  }

  /**
   * @param cls
   */
  def reducer_=[KEYIN <: Writable: Manifest, VALUEIN <: Writable: Manifest, KEYOUT <: Writable: Manifest, VALUEOUT <: Writable: Manifest](cls: Class[_ <: Reducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT]]) {
    job.setReducerClass(cls)
    mapOutputKey = manifest[KEYIN].erasure.asInstanceOf[Class[KEYIN]]
    mapOutputValue = manifest[VALUEIN].erasure.asInstanceOf[Class[VALUEIN]]
    outputKey = manifest[KEYOUT].erasure.asInstanceOf[Class[KEYOUT]]
    outputValue = manifest[VALUEOUT].erasure.asInstanceOf[Class[VALUEOUT]]
  }

  /**
   * @return
   */
  def reducer = job.getReducerClass

  /**
   * @param cls
   * @return
   */
  def reducer[KEYIN <: Writable: Manifest, VALUEIN <: Writable: Manifest, KEYOUT <: Writable: Manifest, VALUEOUT <: Writable: Manifest](cls: Class[_ <: Reducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT]]): Job = {
    reducer = cls
    this
  }

  /**
   * @param tasks
   */
  def numReduceTasks_=(tasks: Int) {
    job.setNumReduceTasks(tasks)
  }

  /**
   * @return
   */
  def numReduceTasks = job.getNumReduceTasks

  /**
   * @param tasks
   * @return
   */
  def numReduceTasks(tasks: Int): Job = {
    numReduceTasks = tasks
    this
  }

  /**
   * @param cls
   */
  def outputKey_=(cls: Class[_ <: Writable]) {
    job.setOutputKeyClass(cls)
  }

  /**
   * @return
   */
  def outputKey = job.getOutputKeyClass

  /**
   * @param cls
   * @return
   */
  def outputKey(cls: Class[_ <: Writable]): Job = {
    outputKey = cls
    this
  }

  /**
   * @param cl
   */
  def outputValue_=(cls: Class[_ <: Writable]) {
    job.setOutputValueClass(cls)
  }

  /**
   * @return
   */
  def outputValue = job.getOutputValueClass

  /**
   * @param cls
   * @return
   */
  def outputValue(cls: Class[_ <: Writable]): Job = {
    outputValue = cls
    this
  }

  /**
   * @param cls
   */
  def outputFormat_=[KEY <: Writable: Manifest, VALUE <: Writable: Manifest](cls: Class[_ <: OutputFormat[KEY, VALUE]]) {
    job.setOutputFormatClass(cls)
    outputKey = manifest[KEY].erasure.asInstanceOf[Class[KEY]]
    outputValue = manifest[VALUE].erasure.asInstanceOf[Class[VALUE]]
  }

  /**
   * @return
   */
  def outputFormat = job.getOutputFormatClass

  /**
   * @param cls
   * @return
   */
  def outputFormat[KEY <: Writable: Manifest, VALUE <: Writable: Manifest](cls: Class[_ <: OutputFormat[KEY, VALUE]]): Job = {
    outputFormat = cls
    this
  }

}

/**
 * @author ueshin
 */
object Job {

  /**
   * @param cls
   * @param init
   * @return
   */
  def apply(cls: Class[_])(init: Job => Unit) = {
    val job = new mapreduce.Job()
    job.setJarByClass(cls)
    init(new Job(job))
    job
  }

  /**
   * @param conf
   * @param cls
   * @param init
   * @return
   */
  def apply(conf: Configuration, cls: Class[_])(init: Job => Unit) = {
    val job = new mapreduce.Job(conf)
    job.setJarByClass(cls)
    init(new Job(job))
    job
  }

  /**
   * @param conf
   * @param jobName
   * @param cls
   * @param init
   * @return
   */
  def apply(conf: Configuration, jobName: String, cls: Class[_])(init: Job => Unit) = {
    val job = new mapreduce.Job(conf, jobName)
    job.setJarByClass(cls)
    init(new Job(job))
    job
  }

  implicit def toMapReduceJob(job: Job) = job.job

}
