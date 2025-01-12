
# parsetab.py
# This file is automatically generated. Do not edit.
# pylint: disable=W,C,R
_tabversion = '3.10'

_lr_method = 'LALR'

_lr_signature = 'cypherADDITION AND AS COLLECT COLON COMMA DASH DISTINCT DISTINCT DIVIDE DOT DQUOTE EQUALS FLOAT GREATERTHAN INTEGER LCURLY LESSTHAN LPAREN LSQUARE MATCH NOT OR RCURLY RETURN RPAREN RSQUARE STRING WHERE WITH WORDcypher : queryquery : match_pattern returnstring : STRINGinteger : INTEGERfloat : FLOATname_label : WORD\n    | WORD COLON WORD\n    | COLON WORDmapping_list : WORD COLON literal\n    | mapping_list COMMA WORD COLON literal\n    node : LPAREN name_label RPAREN\n    | LPAREN name_label LCURLY mapping_list RCURLY RPAREN\n    | LPAREN RPAREN\n    | LPAREN WORD RPAREN\n    alias : WORD AS WORD\n    | object_attribute_lookup AS WORD\n    | aggregation AS WORDliteral : integer\n    | float\n    | STRING\n    relationship : LSQUARE WORD RSQUARE\n    | LSQUARE name_label RSQUAREleft_right : DASH relationship DASH GREATERTHANright_left : LESSTHAN DASH relationship DASHincomplete_relationship_chain : node left_right\n    | node right_left\n    | incomplete_relationship_chain node left_right\n    | incomplete_relationship_chain node right_left\n    relationship_chain : incomplete_relationship_chain noderelationship_chain_list : relationship_chain\n    | relationship_chain_list COMMA relationship_chainwith_as_series : alias\n    | with_as_series COMMA aliascollect : COLLECT LPAREN object_attribute_lookup RPARENaggregation : collect\n    | DISTINCT aggregationwith_clause : WITH with_as_seriesmatch_pattern : MATCH node\n    | MATCH relationship_chain_list\n    | MATCH relationship_chain_list with_clause\n    | MATCH relationship_chain_list where\n    | MATCH relationship_chain_list with_clause where\n    | MATCH node where\n    | MATCH node with_clause where\n    binary_operator : EQUALS\n    | LESSTHAN\n    | GREATERTHAN\n    | OR\n    | ANDbinary_function : ADDITIONaliased_name : WORDpredicate : object_attribute_lookup binary_operator literal\n    | object_attribute_lookup binary_operator object_attribute_lookup\n    | aliased_name binary_operator literal\n    | object_attribute_lookup binary_operator binary_expressionbinary_expression : object_attribute_lookup binary_function literal\n    | object_attribute_lookup binary_function object_attribute_lookup\n    | aliased_name binary_function literal\n    | literal binary_function literalobject_attribute_lookup : WORD DOT WORD\n    | WORDwhere : WHERE predicate\n    | where COMMA predicateprojection : object_attribute_lookup\n    | alias\n    | projection COMMA alias\n    | projection COMMA object_attribute_lookupreturn : RETURN projection'
    
_lr_action_items = {'MATCH':([0,],[4,]),'$end':([1,2,5,12,13,14,15,51,66,67,68,69,70,71,],[0,-1,-2,-68,-64,-65,-61,-61,-66,-67,-16,-60,-15,-17,]),'RETURN':([3,7,8,10,20,28,29,32,35,44,45,48,49,50,56,57,59,61,68,69,70,71,73,74,91,92,93,94,95,96,98,99,100,101,113,116,117,118,119,],[6,-38,-39,-30,-43,-40,-41,-13,-29,-44,-62,-61,-37,-32,-42,-31,-11,-14,-16,-60,-15,-17,-61,-63,-53,-52,-55,-18,-19,-20,-4,-5,-54,-33,-12,-57,-56,-59,-58,]),'LPAREN':([4,11,19,22,23,30,64,65,102,105,],[9,9,42,-25,-26,9,-27,-28,-23,-24,]),'WORD':([6,9,24,25,34,36,37,38,39,40,42,43,54,60,62,75,76,77,78,79,80,82,107,109,110,],[15,33,48,51,63,51,68,69,70,71,73,48,84,88,89,48,-45,-46,-47,-48,-49,51,114,73,-50,]),'DISTINCT':([6,18,25,36,82,],[18,18,18,18,18,]),'COLLECT':([6,18,25,36,82,],[19,19,19,19,19,]),'WHERE':([7,8,10,21,28,32,35,49,50,57,59,61,68,70,71,101,113,],[24,24,-30,24,24,-13,-29,-37,-32,-31,-11,-14,-16,-15,-17,-33,-12,]),'WITH':([7,8,10,32,35,57,59,61,113,],[25,25,-30,-13,-29,-31,-11,-14,-12,]),'DASH':([7,27,32,35,53,58,59,61,86,103,104,113,],[26,55,-13,26,83,26,-11,-14,105,-21,-22,-12,]),'LESSTHAN':([7,32,35,46,47,48,58,59,61,69,113,],[27,-13,27,77,77,-51,27,-11,-14,-60,-12,]),'COMMA':([8,10,12,13,14,15,20,29,32,35,44,45,48,49,50,51,56,57,59,61,66,67,68,69,70,71,73,74,87,91,92,93,94,95,96,98,99,100,101,113,115,116,117,118,119,121,],[30,-30,36,-64,-65,-61,43,43,-13,-29,43,-62,-61,82,-32,-61,43,-31,-11,-14,-66,-67,-16,-60,-15,-17,-61,-63,107,-53,-52,-55,-18,-19,-20,-4,-5,-54,-33,-12,-9,-57,-56,-59,-58,-10,]),'RPAREN':([9,31,33,63,69,72,73,89,106,],[32,59,61,-8,-60,90,-61,-7,113,]),'COLON':([9,33,54,84,88,114,],[34,62,34,62,108,120,]),'AS':([13,15,16,17,41,51,52,67,69,90,],[37,39,40,-35,-36,39,37,37,-60,-34,]),'DOT':([15,48,51,73,],[38,38,38,38,]),'LSQUARE':([26,55,],[54,54,]),'LCURLY':([31,33,63,89,],[60,-6,-8,-7,]),'EQUALS':([46,47,48,69,],[76,76,-51,-60,]),'GREATERTHAN':([46,47,48,69,83,],[78,78,-51,-60,102,]),'OR':([46,47,48,69,],[79,79,-51,-60,]),'AND':([46,47,48,69,],[80,80,-51,-60,]),'ADDITION':([48,69,91,92,94,95,96,97,98,99,],[-51,-60,110,110,-18,-19,-20,110,-4,-5,]),'RSQUARE':([63,84,85,89,],[-8,103,104,-7,]),'STRING':([75,76,77,78,79,80,81,108,109,110,111,112,120,],[96,-45,-46,-47,-48,-49,96,96,96,-50,96,96,96,]),'INTEGER':([75,76,77,78,79,80,81,108,109,110,111,112,120,],[98,-45,-46,-47,-48,-49,98,98,98,-50,98,98,98,]),'FLOAT':([75,76,77,78,79,80,81,108,109,110,111,112,120,],[99,-45,-46,-47,-48,-49,99,99,99,-50,99,99,99,]),'RCURLY':([87,94,95,96,98,99,115,121,],[106,-18,-19,-20,-4,-5,-9,-10,]),}

_lr_action = {}
for _k, _v in _lr_action_items.items():
   for _x,_y in zip(_v[0],_v[1]):
      if not _x in _lr_action:  _lr_action[_x] = {}
      _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'cypher':([0,],[1,]),'query':([0,],[2,]),'match_pattern':([0,],[3,]),'return':([3,],[5,]),'node':([4,11,30,],[7,35,58,]),'relationship_chain_list':([4,],[8,]),'relationship_chain':([4,30,],[10,57,]),'incomplete_relationship_chain':([4,30,],[11,11,]),'projection':([6,],[12,]),'object_attribute_lookup':([6,24,25,36,42,43,75,82,109,],[13,46,52,67,72,46,91,52,116,]),'alias':([6,25,36,82,],[14,50,66,101,]),'aggregation':([6,18,25,36,82,],[16,41,16,16,16,]),'collect':([6,18,25,36,82,],[17,17,17,17,17,]),'where':([7,8,21,28,],[20,29,44,56,]),'with_clause':([7,8,],[21,28,]),'left_right':([7,35,58,],[22,64,22,]),'right_left':([7,35,58,],[23,65,23,]),'name_label':([9,54,],[31,85,]),'predicate':([24,43,],[45,74,]),'aliased_name':([24,43,75,],[47,47,97,]),'with_as_series':([25,],[49,]),'relationship':([26,55,],[53,86,]),'binary_operator':([46,47,],[75,81,]),'mapping_list':([60,],[87,]),'literal':([75,81,108,109,111,112,120,],[92,100,115,117,118,119,121,]),'binary_expression':([75,],[93,]),'integer':([75,81,108,109,111,112,120,],[94,94,94,94,94,94,94,]),'float':([75,81,108,109,111,112,120,],[95,95,95,95,95,95,95,]),'binary_function':([91,92,97,],[109,111,112,]),}

_lr_goto = {}
for _k, _v in _lr_goto_items.items():
   for _x, _y in zip(_v[0], _v[1]):
       if not _x in _lr_goto: _lr_goto[_x] = {}
       _lr_goto[_x][_k] = _y
del _lr_goto_items
_lr_productions = [
  ("S' -> cypher","S'",1,None,None,None),
  ('cypher -> query','cypher',1,'p_cypher','cypher_parser.py',66),
  ('query -> match_pattern return','query',2,'p_query','cypher_parser.py',76),
  ('string -> STRING','string',1,'p_string','cypher_parser.py',81),
  ('integer -> INTEGER','integer',1,'p_integer','cypher_parser.py',86),
  ('float -> FLOAT','float',1,'p_float','cypher_parser.py',91),
  ('name_label -> WORD','name_label',1,'p_name_label','cypher_parser.py',96),
  ('name_label -> WORD COLON WORD','name_label',3,'p_name_label','cypher_parser.py',97),
  ('name_label -> COLON WORD','name_label',2,'p_name_label','cypher_parser.py',98),
  ('mapping_list -> WORD COLON literal','mapping_list',3,'p_mapping_list','cypher_parser.py',112),
  ('mapping_list -> mapping_list COMMA WORD COLON literal','mapping_list',5,'p_mapping_list','cypher_parser.py',113),
  ('node -> LPAREN name_label RPAREN','node',3,'p_node','cypher_parser.py',125),
  ('node -> LPAREN name_label LCURLY mapping_list RCURLY RPAREN','node',6,'p_node','cypher_parser.py',126),
  ('node -> LPAREN RPAREN','node',2,'p_node','cypher_parser.py',127),
  ('node -> LPAREN WORD RPAREN','node',3,'p_node','cypher_parser.py',128),
  ('alias -> WORD AS WORD','alias',3,'p_alias','cypher_parser.py',148),
  ('alias -> object_attribute_lookup AS WORD','alias',3,'p_alias','cypher_parser.py',149),
  ('alias -> aggregation AS WORD','alias',3,'p_alias','cypher_parser.py',150),
  ('literal -> integer','literal',1,'p_literal','cypher_parser.py',155),
  ('literal -> float','literal',1,'p_literal','cypher_parser.py',156),
  ('literal -> STRING','literal',1,'p_literal','cypher_parser.py',157),
  ('relationship -> LSQUARE WORD RSQUARE','relationship',3,'p_relationship','cypher_parser.py',163),
  ('relationship -> LSQUARE name_label RSQUARE','relationship',3,'p_relationship','cypher_parser.py',164),
  ('left_right -> DASH relationship DASH GREATERTHAN','left_right',4,'p_left_right','cypher_parser.py',172),
  ('right_left -> LESSTHAN DASH relationship DASH','right_left',4,'p_right_left','cypher_parser.py',177),
  ('incomplete_relationship_chain -> node left_right','incomplete_relationship_chain',2,'p_incomplete_relationship_chain','cypher_parser.py',182),
  ('incomplete_relationship_chain -> node right_left','incomplete_relationship_chain',2,'p_incomplete_relationship_chain','cypher_parser.py',183),
  ('incomplete_relationship_chain -> incomplete_relationship_chain node left_right','incomplete_relationship_chain',3,'p_incomplete_relationship_chain','cypher_parser.py',184),
  ('incomplete_relationship_chain -> incomplete_relationship_chain node right_left','incomplete_relationship_chain',3,'p_incomplete_relationship_chain','cypher_parser.py',185),
  ('relationship_chain -> incomplete_relationship_chain node','relationship_chain',2,'p_relationship_chain','cypher_parser.py',198),
  ('relationship_chain_list -> relationship_chain','relationship_chain_list',1,'p_relationship_chain_list','cypher_parser.py',203),
  ('relationship_chain_list -> relationship_chain_list COMMA relationship_chain','relationship_chain_list',3,'p_relationship_chain_list','cypher_parser.py',204),
  ('with_as_series -> alias','with_as_series',1,'p_with_as_series','cypher_parser.py',224),
  ('with_as_series -> with_as_series COMMA alias','with_as_series',3,'p_with_as_series','cypher_parser.py',225),
  ('collect -> COLLECT LPAREN object_attribute_lookup RPAREN','collect',4,'p_collect','cypher_parser.py',235),
  ('aggregation -> collect','aggregation',1,'p_aggregation','cypher_parser.py',240),
  ('aggregation -> DISTINCT aggregation','aggregation',2,'p_aggregation','cypher_parser.py',241),
  ('with_clause -> WITH with_as_series','with_clause',2,'p_with_clause','cypher_parser.py',249),
  ('match_pattern -> MATCH node','match_pattern',2,'p_match_pattern','cypher_parser.py',254),
  ('match_pattern -> MATCH relationship_chain_list','match_pattern',2,'p_match_pattern','cypher_parser.py',255),
  ('match_pattern -> MATCH relationship_chain_list with_clause','match_pattern',3,'p_match_pattern','cypher_parser.py',256),
  ('match_pattern -> MATCH relationship_chain_list where','match_pattern',3,'p_match_pattern','cypher_parser.py',257),
  ('match_pattern -> MATCH relationship_chain_list with_clause where','match_pattern',4,'p_match_pattern','cypher_parser.py',258),
  ('match_pattern -> MATCH node where','match_pattern',3,'p_match_pattern','cypher_parser.py',259),
  ('match_pattern -> MATCH node with_clause where','match_pattern',4,'p_match_pattern','cypher_parser.py',260),
  ('binary_operator -> EQUALS','binary_operator',1,'p_binary_operator','cypher_parser.py',273),
  ('binary_operator -> LESSTHAN','binary_operator',1,'p_binary_operator','cypher_parser.py',274),
  ('binary_operator -> GREATERTHAN','binary_operator',1,'p_binary_operator','cypher_parser.py',275),
  ('binary_operator -> OR','binary_operator',1,'p_binary_operator','cypher_parser.py',276),
  ('binary_operator -> AND','binary_operator',1,'p_binary_operator','cypher_parser.py',277),
  ('binary_function -> ADDITION','binary_function',1,'p_binary_function','cypher_parser.py',282),
  ('aliased_name -> WORD','aliased_name',1,'p_aliased_name','cypher_parser.py',287),
  ('predicate -> object_attribute_lookup binary_operator literal','predicate',3,'p_predicate','cypher_parser.py',292),
  ('predicate -> object_attribute_lookup binary_operator object_attribute_lookup','predicate',3,'p_predicate','cypher_parser.py',293),
  ('predicate -> aliased_name binary_operator literal','predicate',3,'p_predicate','cypher_parser.py',294),
  ('predicate -> object_attribute_lookup binary_operator binary_expression','predicate',3,'p_predicate','cypher_parser.py',295),
  ('binary_expression -> object_attribute_lookup binary_function literal','binary_expression',3,'p_binary_expression','cypher_parser.py',305),
  ('binary_expression -> object_attribute_lookup binary_function object_attribute_lookup','binary_expression',3,'p_binary_expression','cypher_parser.py',306),
  ('binary_expression -> aliased_name binary_function literal','binary_expression',3,'p_binary_expression','cypher_parser.py',307),
  ('binary_expression -> literal binary_function literal','binary_expression',3,'p_binary_expression','cypher_parser.py',308),
  ('object_attribute_lookup -> WORD DOT WORD','object_attribute_lookup',3,'p_object_attribute_lookup','cypher_parser.py',316),
  ('object_attribute_lookup -> WORD','object_attribute_lookup',1,'p_object_attribute_lookup','cypher_parser.py',317),
  ('where -> WHERE predicate','where',2,'p_where','cypher_parser.py',325),
  ('where -> where COMMA predicate','where',3,'p_where','cypher_parser.py',326),
  ('projection -> object_attribute_lookup','projection',1,'p_projection','cypher_parser.py',334),
  ('projection -> alias','projection',1,'p_projection','cypher_parser.py',335),
  ('projection -> projection COMMA alias','projection',3,'p_projection','cypher_parser.py',336),
  ('projection -> projection COMMA object_attribute_lookup','projection',3,'p_projection','cypher_parser.py',337),
  ('return -> RETURN projection','return',2,'p_return','cypher_parser.py',346),
]
