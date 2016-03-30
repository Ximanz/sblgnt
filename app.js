var MongoClient = require('mongodb').MongoClient;
var lineReader = require('line-reader');
var fs = require('fs');
var async = require('async');

const url = "mongodb://ximanz:5a71b8700d@ds023078.mlab.com:23078/sblgnt";

const Parts = {
    'A-' : "adjective",
    'C-' : "conjunction",
    'D-' : "adverb",
    'I-' : "interjection",
    'N-' : "noun",
    'P-' : "preposition",
    'RA' : "definite article",
    'RD' : "demonstrative pronoun",
    'RI' : "interrogative/indefinite pronoun",
    'RP' : "personal pronoun",
    'RR' : "relative pronoun",
    'V-' : "verb",
    'X-' : "particle"
};

const Person = {
    "1" : "1st",
    "2" : "2nd",
    "3" : "3rd"
};

const Tense = {
    "P" : "present",
    "I" : "imperfect",
    "F" : "future",
    "A" : "aorist",
    "X" : "perfect",
    "Y" : "pluperfect"
};

const Voice = {
    "A" : "active",
    "M" : "middle",
    "P" : "passive"
};

const Mood = {
    "I" : "indicative",
    "D" : "imperative",
    "S" : "subjunctive",
    "O" : "optative",
    "N" : "infinitive",
    "P" : "participle"
};

const Case = {
    "N" : "nominative",
    "G" : "genitive",
    "D" : "dative",
    "A" : "accusative"
};

const Number = {
    "S" : "singular",
    "P" : "plural"
};

const Gender = {
    "M" : "masculine",
    "F" : "feminine",
    "N" : "neuter"
};

const Degree = {
    "C" : "comparative",
    "S" : "superlative"
};

MongoClient.connect(url, function(err, db) {
    var wordCollection = db.collection('words');
    var verseCollection = db.collection('verses');
    var docs = [];
    var verses = [];

    console.log('mongo connected');

    fs.readdir("./", function(err, files) {
        if( err ) {
            console.error( "Could not list the directory.", err );
            process.exit( 1 );
        }

        async.eachSeries(files, function(file, callback) {
            if (file.endsWith('morphgnt.txt')) {
                console.log("Processing " + file);
                var lastref = '';
                var index = 1;
                var verse = {_id: '', words: []};
                lineReader.eachLine(file, function(line) {
                    var elements = line.split(' ');
                    if (elements.length < 7) {
                        console.log('incorrect number of elements');
                        return;
                    }

                    if (lastref != elements[0]) {
                        lastref = elements[0];
                        index = 1;

                        if (verse._id != '') verses.push(verse);
                        verse = {_id: elements[0], words: []};
                    }

                    docs.push({
                        _id: (elements[0] + ("00" + index).slice(-2)),
                        ref: elements[0],
                        part: {
                            code: elements[1],
                            description: Parts[elements[1]]
                        },
                        parse: elements[2].split("").map(function(item, index, array) {
                            if (item == "-") return null;
                            switch(index) {
                                case 0:
                                    return {code: item, description: Person[item]};
                                case 1:
                                    return {code: item, description: Tense[item]};
                                case 2:
                                    return {code: item, description: Voice[item]};
                                case 3:
                                    return {code: item, description: Mood[item]};
                                case 4:
                                    return {code: item, description: Case[item]};
                                case 5:
                                    return {code: item, description: Number[item]};
                                case 6:
                                    return {code: item, description: Gender[item]};
                                case 7:
                                    return {code: item, description: Degree[item]};
                            }
                        }),
                        manuscript: elements[3],
                        word: elements[4],
                        normalised: elements[5],
                        lemma: elements[6]
                    });

                    verse.words.push((elements[0] + ("00" + index).slice(-2)));

                    index++;

                    if (docs.length == 1000) {
                        console.log('inserting 1000 lines');
                        wordCollection.insertMany(docs, {w: 1}, function(err) {
                            if (err) {
                                console.error("error", err)
                            }
                        });
                        docs.length = 0;
                    }
                }, function (err) {
                    if (err) {
                        console.error("error", err)
                    }

                    async.series([
                        function(callback) {
                            console.log('inserting ' + docs.length + ' lines...');
                            wordCollection.insertMany(docs, {w: 1}, function(err) {
                                if (err) {
                                    console.error("error", err)
                                }
                                docs.length = 0;
                                callback();
                            });
                        },
                        function(callback) {
                            verses.push(verse);
                            verseCollection.insertMany(verses, {w: 1}, function(err) {
                                if (err) {
                                    console.error("error", err)
                                }
                                verses.length = 0;
                                verse = {_id: '', words: []};
                                callback();
                            })
                        }
                    ], function() {
                        return callback();
                    });
                });
            } else {
                return callback();
            }
        }, function (err) {
            if (err) {
                console.error("error", err)
            }
            console.log('creating indexes start');

            async.series([
                function(callback) {
                    console.log('creating index for ref..');
                    wordCollection.createIndex({'ref': 1}, function (err) {
                        if (err) {
                            console.error("error", err)
                        }
                        callback();
                    });
                },
                function(callback) {
                    console.log('creating index for lemma..');
                    wordCollection.createIndex({'lemma': 1}, function (err) {
                        if (err) {
                            console.error("error", err)
                        }
                        callback();
                    });
                }
            ], function() {
                db.close();
            });
        });
    });
});