# PEPM19-supplementary-material-scala

Scala implementation of <https://github.com/sulzmann/PEPM19-supplementary-material>.

## Automatic Build with TravisCI
[![Build Status](https://travis-ci.org/tdauth/PEPM19-supplementary-material-scala.svg?branch=master)](https://travis-ci.org/tdauth/PEPM19-supplementary-material-scala)
[![Code Coverage](https://img.shields.io/codecov/c/github/tdauth/PEPM19-supplementary-material-scala/master.svg)](https://codecov.io/github/tdauth/PEPM19-supplementary-material-scala?branch=master)

## Benchmarks
Call `sbt run` to execute the benchmarks described in our short paper.
They will create text files into the current directory with the measured execution times.

## Program Transformations
The invalid program transformations described in our short paper are shown in the package [programtransformations](./src/main/scala/tdauth/pepm19/programtransformations).