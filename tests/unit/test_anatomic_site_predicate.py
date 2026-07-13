from app.utils.cypher_builder import anatomic_site_member_predicate


def test_member_predicate_is_list_native():
    pred = anatomic_site_member_predicate("sa", "$_anatomical_sites_param")
    assert "SPLIT" not in pred.upper()
    assert "reduce(" not in pred
    assert "coalesce(sa.anatomic_site, [])" in pred
    assert "$_anatomical_sites_param" in pred


def test_member_predicate_respects_node_var_and_param():
    pred = anatomic_site_member_predicate("d", "$_anatomical_sites_0")
    assert "coalesce(d.anatomic_site, [])" in pred
    assert "$_anatomical_sites_0" in pred
