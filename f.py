import fdb

fdb.api_version(710)


@fdb.transactional
def set_county_population(tr, state, county, pop):
    tr[fdb.tuple.pack((state, county))] = str(pop)


@fdb.transactional
def get_county_populations_in_state(tr, state):
    return [int(pop) for k, pop in tr[fdb.tuple.range((state,))]]
