#!/usr/bin/python
# -*- coding: utf-8 -*-

from db_manager import DBmanager
from fill_db import DBfill
from consultas import Consultas
import rethinkdb as r
from rethinkdb.errors import *

def test_consulta_1(data_base):
	consultas = Consultas(data_base.db_name, data_base.connection)
	resultado = consultas.enfrentamientos_ganados_por_competidor_campeonato(2016)
	print(resultado)

def test_consulta_2(data_base):
	consultas = Consultas(data_base.db_name, data_base.connection)
	medallas = consultas.cantidad_medallas_escuela()
	print(medallas)

def test_consulta_3(data_base):
	consultas = Consultas(data_base.db_name, data_base.connection)
	resultado = consultas.campeonato_mas_medallas_escuela()
	print(resultado)

def test_consulta_4(data_base):
	consultas = Consultas(data_base.db_name, data_base.connection)
	arbitro = consultas.arbitros_con_mas_de_4_campeonatos()
	print(arbitro)

def test_consulta_5(data_base):
	consultas = Consultas(data_base.db_name, data_base.connection)
	resultado = consultas.escuela_mayor_numero_comp_por_campeonato()
	print(resultado)

def test_consulta_6(data_base):
	consultas = Consultas(data_base.db_name, data_base.connection)
	resultado = consultas.competidor_mas_medallas_por_modalidad()
	print(resultado)


if __name__ == "__main__":
	fill = DBfill(3, 10, 10, 2)
	fill.load_db()

	print("test_consulta_1")
	test_consulta_1(fill.d)

	print("test_consulta_2")
	test_consulta_2(fill.d)

	print("test_consulta_3")
	test_consulta_3(fill.d)

	print("test_consulta_4")
	test_consulta_4(fill.d)

	print("test_consulta_5")
	test_consulta_5(fill.d)

	print("test_consulta_6")
	test_consulta_6(fill.d)

