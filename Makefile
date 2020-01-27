EXTENSION = check_orapg
DATA = check_orapg--2.0.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
