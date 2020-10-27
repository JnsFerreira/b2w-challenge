# B2W Data Engineer Challange

<img src="https://upload.wikimedia.org/wikipedia/commons/e/e3/B2W_Digital_logo.png?raw=true" width="200" height="200"/>

## Description

Develop a job to find carts abandoned by e-commerce customers. Although the subject is rich, in the test the definition of cart abandonment will be simplified.

## Enviroment

OS: Ubuntu

OS Version: 20.04.1 LTS x64

Python Version: 3.8.5

Apache Beam Version: 2.25.0

## Requirements

To run you will need git pkg and virtualenv python library

1. Installing git via apt

```
$ sudo apt-get install git
```
2. Installing Python virtualenv lib

```
$ pip3 install virtualenv
```

## Setup

Open your terminal and follow the steps:

1. Clone the repository:

```
$ git clone https://github.com/JnsFerreira/b2w_challange.git
```

2. Change to repo directory
```
$ cd b2w_challange
```

3. Activate the virtual enviroment
```
$ source b2w_challange_env/bin/activate
```

## Running

If you want to use default input and output directories:

```
python3 abandoned_carts.py
```

Otherwise, to specify input and output directories:

```
python3 abandoned_carts.py --input [path_to_input] --ouput [path_to_output]
```

## Checking the output file

To see the output file content, type on your terminal:

```
$ cat -n output/abandoned_carts-00000-of-00001.json
```

## Referecences

* [Ubuntu](https://ubuntu.com/)
* [Python 3](https://www.python.org/about/)
* [Apache Beam](https://beam.apache.org/)
