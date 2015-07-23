exports.printMsg = function (color) {

    var message = "This is another message from sri2postgres.";

    if (color === undefined) {
        console.log(message);
    }else{
        console.warn(message);
    }
}