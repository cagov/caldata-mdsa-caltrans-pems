

    

CREATE OR REPLACE FUNCTION ANALYTICS_PRD.public.exponential_smooth("VALUE" FLOAT, "FACTOR" FLOAT)
RETURNS TABLE ("VALUE_SMOOTHED" FLOAT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'Smoother'
AS $$
class Smoother:
    def __init__(self):
        self.previous_value = None

    def process(self, value, factor):
        if value is None or factor is None:
            yield_value = None
        else:
            # If previous value was null, substitute current value.
            previous_value = self.previous_value if self.previous_value is not None else value
            yield_value = value * factor + (1-factor) * previous_value

        self.previous_value = yield_value
        yield (yield_value,)
$$

;

