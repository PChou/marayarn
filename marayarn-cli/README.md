# Command-Line Interface

Marayarn provides a Command-Line Interface (CLI) to manage applications on yarn. 

The command line can be used to:
- submit application,
- kill application,
- get status of application,
- provide information about application,
- scale application

## Example

#### submit application
```
./marayarn submit \
-cpu 1 -memory 512 -name hello_world -instance 2 \
-cmd 'while true; do date; sleep 5; done'
```

#### kill application
```
./marayarn kill -app <appID>
```

#### status application
```
./marayarn status -app <appID>
```

#### info application
```
./marayarn info -app <appID>
```

#### scale application
```
./marayarn scale -app <appID> -instance 3
```

## Usage

The command line syntax is as follows:

./marayarn <ACTION> [OPTIONS]

The following actions are available:

- submit
- kill
- status
- info
- scale

Use `-h` to get more information