function mapper(key, value) {
    var words = key.split(' ');
    for (var i = 0; i < words.length; ++i) {
        Wmr.emit(words[i],  '1');
    }
}

function reducer(key, values) {
    var count = 0;
    for (var i = 0; i < values.length; ++i) {
        count += parseInt(values[i], 10);
    }
    Wmr.emit(key, count.toString());
}
