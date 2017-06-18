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
	def enfrentamientos_ganados_por_competidor_campeonato(self, year_campeonato):
		dic_competidor = {}
		iterador_enfretamientos = r.db(self.db_name).table('Enfrentamiento').run(self.connection)
		# Itero sobre los enfrentamientos
		for doc in iterador_enfretamientos:
			if doc.get("yearCampeonato") == year_campeonato:
				competidor = doc.get("DNIGanador")
				if competidor in dic_competidor:
					dic_competidor[competidor] += 1
				else:
					dic_competidor[competidor] = 1	
		iterador_enfretamientos.close()
		return  dic_competidor

	# Sin testear
	def cantidad_medallas_escuela(self):
		dic_escuelas = {}
		iterador_escuelas = r.db(self.db_name).table('Escuela').run(self.connection)
		# Itero sobre las escuelas
		for escuela in iterador_escuelas:		
			medallas = 0
			lista_competidores = escuela.get("ListaCompetidores")
			# Para cada escuela itero sobre sus competidores
			for comp in lista_competidores:
				medallas += len(comp.get("Oro")) + len(comp.get("Plata")) + len(comp.get("Bronce"))
			
			dic_escuelas[escuela.get("NombreEscuela")] = medallas
		
		iterador_escuelas.close()
		return dic_escuelas

	# Sin testear
	def campeonato_mas_medallas_escuela(self):
		dic_escuelas = {}
		iterador_escuelas = r.db(self.db_name).table('Escuela').run(self.connection)
		# Itero sobre las escuelas
		for escuela in iterador_escuelas:
			# Placeholder
			year_campeonato_mas_medallas = 0
			medallas_campeonato_ganador = 0
			lista_campeonatos = escuela.get("ListaCampeonatos")
			# Para cada escuelo itero sobre sus campeonatos
			for year_campeonato in lista_campeonatos:
				medallas_campeonato = 0
				campeonato = r.db(self.db_name).table('Campeonato').get(year_campeonato).run(self.connection)
				lista_categorias = campeonato.get("ListaCategorias")
				# Para cada campeonato itero sobre sus categorias
				for categoria in lista_categorias:
					ganador_oro = categoria.get("GanadorOro")
					ganador_plata = categoria.get("GanadorPlata")
					ganador_bronce = categoria.get("GanadorBronce")
					# Veo si los ganadores de la categoria son de la escuela
					if ganador_oro.get("NombreEscuela") == escuela.get("NombreEscuela"):
						medallas_campeonato += 1
					if ganador_plata.get("NombreEscuela") == escuela.get("NombreEscuela"):
						medallas_campeonato += 1
					if ganador_bronce.get("NombreEscuela") == escuela.get("NombreEscuela"):
						medallas_campeonato += 1
				# Actualizo campeonato mas ganador
				if medallas_campeonato > medallas_campeonato_ganador:
					year_campeonato_mas_medallas = year_campeonato
					medallas_campeonato_ganador = medallas_campeonato
			
			dic_escuelas[escuela.get("NombreEscuela")] = year_campeonato_mas_medallas
		
		iterador_escuelas.close()
		return  dic_escuelas	

	# Sin testear
	def arbitros_con_mas_de_4_campeonatos(self):
		lista_arbitros = []
		iterador_arbitros = r.db(self.db_name).table('Arbitro').run(self.connection)
		for doc in iterador_arbitros:
			cantidad_campeonatos = len(doc.get("ListaCampeonatos"))
			if cantidad_campeonatos >= 4:
				lista_arbitros.append(doc.get("DNIArbitro"))
		iterador_arbitros.close()
		return lista_arbitros
	
	# Sin testear
	def escuela_mayor_numero_comp_por_campeonato(self):
		dic_campeonato = {}
		iterador_campeonatos = r.db(self.db_name).table('Campeonato').run(self.connection)
		# Itero sobre los campeonatos
		for campeonato in iterador_campeonatos:
			nombre_escuela_ganadora = ""
			competidores_escuela_ganador = 0
			lista_escuelas = campeonato.get("ListaEscuelas")
			# Para cada campeonato itero sobre sus escuelas
			for escuela in lista_escuelas:
				competidores_escuela_actual = 0
				lista_competidores = campeonato.get("ListaCompetidores")
				# Para cada competido en campeonato me fijo si pertenece a la escuela
				for competidor in lista_competidores:
					if competidor.get("NombreEscuela") == escuela:
						competidores_escuela_actual += 1
				# Actualizo escuela con mayor competidores en el campeonato
				if competidores_escuela_actual > competidores_escuela_ganador:
					competidores_escuela_ganador = competidores_escuela_actual
					nombre_escuela_ganadora = escuela

			dic_campeonato[campeonato.get("yearCampeonato")] = nombre_escuela_ganadora

		iterador_campeonatos.close()
		return dic_campeonato

if __name__ == "__main__":
	
	data_base = DBmanager()
	#data_base.create_db()
	
	# Arbitros
	# data_base.insert_arbitro(100, "Gerardo", [1, 2, 3, 4])
	# data_base.insert_arbitro(200, "Castrilli", [1, 2])
	# data_base.insert_arbitro(300, "Colina", [1, 2, 3, 4, 5])
	
	prueba = Consultas(data_base.db_name, data_base.connection)

	# Consulta 2.1
	# resul = prueba.enfrentamientos_ganados_por_competidor_campeonato(2016)
	# print(resul)
	
	# Consulta 2.2
	# medallas = prueba.cantidad_medallas_escuela()
	# print("Resultado Consulta 2.2: ", medallas)

	# Consulta 2.3
	# resul = prueba.campeonato_mas_medallas_escuela()
	# print(resul)

	# Consulta 2.4
	# arbitro = prueba.arbitros_con_mas_de_4_campeonatos()
	# print("Resultado Consulta 2.4: ", arbitro)

	# Consulta 2.5
	# resul = prueba.escuela_mayor_numero_comp_por_campeonato()
	# print(resul)

	# Consulta 2.6
	
	# data_base.drop_db()

