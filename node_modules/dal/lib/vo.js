var dal = require("./dal"),
    _ = require("under_score");

// the vo template base class
// vo object keeps reference to table definition stored in dal, so that dal read/write vo without further reference
// to database table.  All dal operation returns or accepts vo as parameter.
// example:
//   new dal.vo.<table>();
//   new dal.vo.<table>({col:val});
//   new dal.vo.<table>({col:val}, false);
// to exclude a member from being included by dal in doing database operation, set member to undefined
function vo() {
    if (arguments.length==0)    // happens when Object.clone(vo) is called
        return;
        
    var self = this;
    this._tableDef = arguments[0];
    this._name = this._tableDef.tableName;
    
    if (arguments.length>3) 
        throw new Error("Too many arguments");
        
    if (arguments.length==1) { // construct a default vo object based on table def
        _(this._tableDef.cols).each(function(col, name){
            self[name] = col.defaultVal;
        });
    }
    else {  // construct based on parameter
        var o = arguments[1];
        if (!_(o).isObject())
            throw new Error("Invalid vo argument, must be an object");
        var check = true;
        if (arguments.length==3) {
            check = arguments[2];
            if (!_(check).isBoolean())
                throw new Error("Invalid vo argument, must be a bool");
        }
        
        var self = this;
        _(o).each(function(val, key) {
            if (key in self._tableDef.cols) {
                // TODO: enum check
                self[key] = val;
            }
            else {
                if (check)
                    throw new Error(_("%s is not a member of %s").format(key, self._tableDef.tableName));
            } 
        });
    }
}

vo.prototype._cols = function() {
    var self = this;
    return _(this).chain().keys().filter(function(k) {return k[0]!='_' && self[k]!==undefined}).value();
}

// return a native js object cloned from this without private vo members
vo.prototype.obj = function() {
    return dal.purifyVo(this);
}

module.exports = vo;