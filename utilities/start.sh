#!/bin/bash

AWS_PROFILE="DSIS_Dev_Russellc" # replace this with a profile name from your ~/.aws/config file
MIN_PYTHON="0312"
VENV="venv$MIN_PYTHON"

clear
echo ""
echo "Starting up environment for DynamoDB"

cd "$(dirname "$0")" || exit
cd ..

## check for minimum python
PYTHON_VERSION="$(python -c 'import sys; version=sys.version_info[:2]; print("{0:02d}{1:02d}".format(*version))')"
if [ "$PYTHON_VERSION" -ge $MIN_PYTHON ]; then

  # create virtual environment if needed
  if [ ! -d ./$VENV ]; then
    echo "."
    python3 -m venv $VENV
  fi

  # activate environment and ensure basic tools
  if [ -d ./$VENV ]; then
    echo "."
    source ./$VENV/bin/activate
    echo "."
    python3 -m pip install -q --upgrade pip
    echo "."
    python3 -m pip install -q --upgrade pip-tools

    # ensure script dependencies
    if [ -f "./config/requirements.in" ]; then
      echo "."
      python3 -m piptools compile -q -U --strip-extras -o "config/requirements.txt" "config/requirements.in"

      if [ -f "./config/requirements.txt" ]; then
        echo "."
        python3 -m pip install -q --upgrade -r "config/requirements.txt"

      else
        echo ""
        echo "Failure. Unable to install python dependencies."
        return

      fi

    else
      echo ""
      echo "Failure. Unable to install python dependencies."
      return

    fi

  else
    echo ""
    echo "Failure. Unable to activate python environment."
    return

  fi

else
  echo ""
  echo "Failure. Python version $PYTHON_VERSION is insufficient."
  return

fi

echo "."
export AWS_PROFILE=$AWS_PROFILE
aws sso login --profile $AWS_PROFILE

# remind user to add temporary credentials
echo "copy the comand block from the access portal > account > role > access keys > option 1"
sleep 10
echo " then paste the block here:"
