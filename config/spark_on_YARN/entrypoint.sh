#!/bin/bash

service ssh start

exec su sparkuser bash -c "tail -F anything"
