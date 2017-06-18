#!/usr/bin/python
# -*- coding: utf-8 -*-

from db_manager import DBmanager
import rethinkdb as r
from rethinkdb.errors import *

class Consultas:

	def __init__(self, db_name, connection):
		self.db_name = db_name
		self.connection = connection

	# Sin testear
	def cantidad_medallas_escuela(self, NombreEscuela):
		medallas = 0
		escuela = r.db(self.db_name).table('Escuela').get(NombreEscuela).run(self.connection)
		lista_competidores = escuela.get("ListaCompetidores")
		for comp in lista_competidores:
			medallas += len(comp.get("Oro")) + len(comp.get("Plata")) + len(comp.get("Bronce"))
		return  medallas

	def arbitros_con_mas_de_4_campeonatos(self):
		lista_arbitros = []
		iterador = r.db(self.db_name).table('Arbitro').run(self.connection)
		for doc in iterador:
			cantidad_campeonatos = len(doc.get("ListaCampeonatos"))
			if cantidad_campeonatos >= 4:
				lista_arbitros.append(doc.get("DNIArbitro"))
		iterador.close()
		return  lista_arbitros

if __name__ == "__main__":
	
	data_base = DBmanager()
	data_base.create_db()
	
	# Arbitros
	data_base.insert_arbitro(100, "Gerardo", [1, 2, 3, 4])
	data_base.insert_arbitro(200, "Castrilli", [1, 2])
	data_base.insert_arbitro(300, "Colina", [1, 2, 3, 4, 5])

	# Escuelas
	data_base.insert_escuela("CNBA")
	data_base.insert_escuela("Pellegrini")
	
	prueba = Consultas(data_base.db_name, data_base.connection)

	# Consulta 2.2
	medallas = prueba.cantidad_medallas_escuela("CNBA")
	print("Resultado Consulta 2.2: ", medallas)

	# Consulta 2.4
	arbitro = prueba.arbitros_con_mas_de_4_campeonatos()
	print("Resultado Consulta 2.4: ", arbitro)

	data_base.drop_db()

