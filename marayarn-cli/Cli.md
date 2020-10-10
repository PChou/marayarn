# Command-Line Interface

Marayarn provides a Command-Line Interface (CLI) to manage applications on yarn. 

The command line can be used to:
- submit a application,
- kill a application,
- get status of application,
- provide information about application,
- scale a application

## Example

#### submit application
```
bin/marayarn submit \
-cpu 1 -memory 512 -name hello_world -instance 2 \
-cmd 'while true; do date; sleep 5; done'
```

#### kill application
```
bin/marayarn kill -app <appID>
```

#### status application
```
bin/marayarn status -app <appID>
```

#### info application
```
bin/marayarn info -app <appID>
```

#### scale application
```
bin/marayarn scale -app <appID> -instance 3
```

## Usage

The command line syntax is as follows:

bin/marayarn <ACTION> [OPTIONS]

The following actions are available:
- submit
- kill
- status
- info
- scale
