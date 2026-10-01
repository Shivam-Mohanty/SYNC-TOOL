-- init_tenant_graph.sql
-- Initializes per-tenant Apache AGE graph with vertex and edge labels
-- Usage: psql -v org_id='<org_id>' -f init_tenant_graph.sql
-- Note: org_id should be hex without hyphens, e.g. org123

LOAD 'age';
SET search_path = ag_catalog, "$user", public;

DO $$
DECLARE
    gname text := 'workspace_graph_' || :'org_id';
BEGIN
    -- Create graph if not exists
    IF NOT EXISTS (SELECT 1 FROM ag_catalog.ag_graph WHERE name = gname) THEN
        PERFORM ag_catalog.create_graph(gname);
    END IF;

    -- Pre-create vertex labels
    PERFORM ag_catalog.create_vlabel(gname, 'Component');
    PERFORM ag_catalog.create_vlabel(gname, 'Decision');
    PERFORM ag_catalog.create_vlabel(gname, 'Constraint');
    PERFORM ag_catalog.create_vlabel(gname, 'Task');

    -- Pre-create edge labels
    PERFORM ag_catalog.create_elabel(gname, 'DEPENDS_ON');
    PERFORM ag_catalog.create_elabel(gname, 'MODIFIES');
    PERFORM ag_catalog.create_elabel(gname, 'CONSTRAINED_BY');
    PERFORM ag_catalog.create_elabel(gname, 'DECIDED_IN');
    PERFORM ag_catalog.create_elabel(gname, 'BLOCKS');
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Graph % or labels already configured or notice: %', gname, SQLERRM;
END $$;
