class OpMonFunction :
    def __init__(self,
                 function,
                 opmon_id : re.Pattern,
                 measurement : re.Pattern) :
        self.function = function
        self.opmon_id = opmon_id
        self.measurement = measurement

    def match(self, key : str) -> bool :
        opmon_id,measure = key.split('/',1)
        if not self.opmon_id.match(opmon_id) : return False
        if not self.measurement.match(measure) : return False
        return True

    def execute(self, e : entry.OpMonEntry ) :
        self.function(e)
