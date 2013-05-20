var default_separator = '\t';
var stdin = process.stdin;

var Wmr = {
    emit :  function(cls, key, value) {
        console.log(key + default_separator + value);
    }
};

var parseInput = function(line, separator) {
    separator = typeof separator !== 'undefined' ? separator : default_separator;
    line.replace(/(\n|\r|\r\n)$/, '');
    return 0;
    //return [line[:line.find(separator)], line[line.find(separator)+1:]];
};