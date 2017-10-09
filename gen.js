
var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var fs = require("fs");
var readline = require("readline");
var dirs = ["TabelaUnificada_201709_v1709011646"];
var filename = "layout.txt"
var tables = [];
var count = 0;
var createCounter = 0;


var connection = new Connection({


});


connection.on('connect', err => {
    if (err)
        throw err


    var readLayout = readline.createInterface({
        input: fs.createReadStream(dirs[0] + "/" + filename),
        output: process.stout,
        console: false
    })

    readLayout.on('line', function(line) {
        convertLine(line);
    });

    readLayout.on('close', function() {
        // createDatabase();
        populateTable();
    })
});

var convertLine = function(line) {
    if (line.length === 0 || line === 'Coluna,Tamanho,Inicio,Fim,Tipo')
        return;
    if (line.indexOf("tb_") === 0 || line.indexOf("rl_") === 0 ) {
            tables.push({name: line});
            return;
    }
    var count = tables.length - 1;
    var column = line.split(",");
    if (tables[count].columnName === undefined) {
        tables[count].columnName = [];
        tables[count].start = [];
        tables[count].end = [];
        tables[count].type = [];
    }
    tables[count].columnName.push(column[0]);
    tables[count].start.push(parseInt(column[2])-1);
    tables[count].end.push(parseInt(column[3])-1);
    if (column[4].indexOf("CHAR") > -1) {
        tables[count].type.push("TEXT");
    } else {
        tables[count].type.push("INT");
    }
}

var createDatabase = function() {
    var statements = [];
    var table;
    var statement;
    for (var i = 0; i < tables.length; i++) {
        table = tables[i];
        statement = "create table " + table.name + " ("
        var columnStmnt = [];
        var size = table.columnName.length;
        for (var j = 0; j < size; j++) {
            columnStmnt.push(table.columnName[j] + " " + table.type[j]);
        }
        statement += columnStmnt.join(",") + ")";
        statements.push(statement);
    }
    execCreateStmnts(statements, populateTable);
}

var execCreateStmnts = function(statements, cb) {
    if (createCounter == statements.length) {
        populateTable();
        return;
    }
    connection.execSql(new Request(statements[createCounter], function(err) {
        if (err)
            throw err;
        console.log('here');
        createCounter++;
        execCreateStmnts(statements, cb);
    }));
}

var counter = 0;
var populateTable = function() {
    if (counter >= tables.length) {
        console.log("done")
        connection.close();
        return;
    }
    var bulkStatement = [];
    var tableName = tables[counter].name;
    var fileName = tables[counter].name;
    if (tableName === 'tb_procedimento' || tableName == 'tb_cid') {
        tableName += '_temp';
    }
    console.log("Starting " + tableName);
    console.log("Remaining " + (tables.length - counter));
    connection.execSql(new Request("truncate table " + tableName, (err) => {
        var readTable = readline.createInterface({
            input: fs.createReadStream(dirs[0] + "/" + fileName + ".txt"),
            output: process.stout,
            console: false
        })

        readTable.on('line', function(line) {
            if (line.length ===  0) return;
            bulkStatement.push(getFields(line))
        });

        readTable.on('close', function() {
            if (bulkStatement.length === 0) {
                counter++;
                return populateTable();
            }
            insertBulk(tableName, bulkStatement, (err, rowCount) => {
                if (err) {
                    throw err;
                }
                counter++
                populateTable();
            });
        })
    }));
}

var insertBulk = function(tableName, statements, cb) {
    var values = [];
    var insertStatement = 'Insert into ' + tableName +
            ' (' + Object.keys(statements[0]).join(',') + ') values ';
    var insertCount = 0;
    for (var i = 0; i < statements.length; i++) {
        values.push('(' + Object.values(statements[i]).join(',') + ')')
        insertCount++;
        if (insertCount === 1000) {
            var request = new Request(insertStatement + values.join(','), (err, rowCount) => {
                insertBulk(tableName, statements.splice(1000), cb);
            });
            connection.execSql(request);
            return;
        }
    }
    var request = new Request(insertStatement + values.join(','), cb);
    connection.execSql(request);
}


var insertEntry = function(line) {
    if (line.length === 0) return;
    connection.execSql(new Request("insert into " + tables[counter].name + " values " + getFields(line), err => {
        if (err) throw err;
    }));
}

var getFields = function(line) {
    var fields = {};
    var table = tables[counter];
    var field;
    var strLine;
    for (var i = 0; i < table.columnName.length; i++) {
        strLine = line.substring(table.start[i], table.end[i] + 1);
        if (table.type[i] === "TEXT") {
            field = "'" + strLine.replace(/\'/g, "''") + "'";
        }
        else {
            field = strLine;
            if (["VL_SA", "VL_SH", "VL_SP"].indexOf(table.columnName[i]) > -1) {
                field = strLine.substring(1,8) + '.' + strLine.substring(8);
            }
        }

        fields[table.columnName[i]] = field;
    }
    return fields;
}





