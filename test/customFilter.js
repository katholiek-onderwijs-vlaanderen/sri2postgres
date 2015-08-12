/**
 * Created by pablo on 12/08/15.
 */
function CustomFilter (){

    this.isValid = function (resource){

        var text = resource.$$expanded.attachments[0].externalUrl;
        return text.indexOf('.doc') >= 0 && text.indexOf('~$') == -1;
    };
};

// export the class
module.exports = CustomFilter;