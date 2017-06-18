#!/usr/bin/python
# -*- coding: utf-8 -*-

from db_manager import DBmanager
import rethinkdb as r
from rethinkdb.errors import *

class Consultas:

	def __init__(self, db_name, connection):
		self.db_name = db_name
		self.connection = connection

	def enfrentamientos_ganados_por_competidor_campeonato(self, year_campeonato):
		iterador_enfretamientos = r.db(self.db_name).table('Enfrentamiento') \
									.filter(r.row["yearCampeonato"] == year_campeonato) \
									.run(self.connection)
		dic_competidor = {}
		# Itero sobre los enfrentamientos
		for doc in iterador_enfretamientos:
			competidor = doc.get("DNIGanador")
			if competidor in dic_competidor:
				dic_competidor[competidor] += 1
			else:
				dic_competidor[competidor] = 1	
		iterador_enfretamientos.close()
		return  dic_competidor

	def cantidad_medallas_escuela(self):
		iterador_escuelas = r.db(self.db_name).table('Escuela').run(self.connection)
		dic_escuelas = {}
		# Itero sobre las escuelas
		for escuela in iterador_escuelas:
			medallas = 0
			lista_competidores = escuela.get("ListaCompetidores")
			# Para cada escuela itero sobre sus competidores
			for comp in lista_competidores:
				medallas += comp.get("Oro") + comp.get("Plata") + comp.get("Bronce")
			
			dic_escuelas[escuela.get("NombreEscuela")] = medallas
		
		iterador_escuelas.close()
		return dic_escuelas

	def campeonato_mas_medallas_escuela(self):
		iterador_escuelas = r.db(self.db_name).table('Escuela').run(self.connection)
		iterador_campeonatos = r.db(self.db_name).table('Campeonato').run(self.connection)
		# Guardo los campeonatos en un diccionario
		dic_campeonatos = {}
		for campeonato in iterador_campeonatos:
			year = campeonato.get("yearCampeonato")
			dic_campeonatos[year] = campeonato.get("ListaCategorias")
		dic_escuelas = {}
		# Itero sobre las escuelas
		for escuela in iterador_escuelas:
			# Placeholder
			year_con_mas_medallas = 0
			mayor_cantidad_de_medallas_en_un_campeonato = 0
			lista_campeonatos = escuela.get("ListaCampeonatos")
			# Para cada escuelo itero sobre sus campeonatos
			for year_campeonato in lista_campeonatos:
				medallas_campeonato = 0
				lista_categorias = dic_campeonatos[year_campeonato]
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
				if medallas_campeonato > mayor_cantidad_de_medallas_en_un_campeonato:
					year_con_mas_medallas = year_campeonato
					mayor_cantidad_de_medallas_en_un_campeonato = medallas_campeonato
			
			dic_escuelas[escuela.get("NombreEscuela")] = year_con_mas_medallas
		
		iterador_escuelas.close()
		iterador_campeonatos.close()
		return  dic_escuelas

	def arbitros_con_mas_de_4_campeonatos(self):
		iterador_arbitros = r.db(self.db_name).table('Arbitro') \
									.filter(r.row["ListaCampeonatos"].count() > 3) \
									.map(lambda arbitro: arbitro["DNIArbitro"]) \
									.run(self.connection)
		iterador_arbitros.close()
		return list(iterador_arbitros)
	
	def escuela_mayor_numero_comp_por_campeonato(self):
		dic_campeonato = {}
		iterador_campeonatos = r.db(self.db_name).table('Campeonato').run(self.connection)
		# Itero sobre los campeonatos
		for campeonato in iterador_campeonatos:
			dic_escuelas = {}
			lista_competidores = campeonato.get("ListaCompetidores")
			competidores_escuela_ganador = 0
			# Para cada campeonato itero sobre sus competidores
			for competidor in lista_competidores:
				escuela_competidor = competidor.get("NombreEscuela")
				if escuela_competidor in dic_escuelas:
					dic_escuelas[escuela_competidor] += 1
				else:
					dic_escuelas[escuela_competidor] = 1
			# Si hay empate, me quedo con uno
			dic_campeonato[campeonato.get("yearCampeonato")] = max(dic_escuelas, key=dic_escuelas.get)

		iterador_campeonatos.close()
		return dic_campeonato

	def competidor_mas_medallas_por_modalidad(self):
		dic_de_dic__modalidad = {'Formas': {}, 'Combate': {}, 'Rotura':{}}
		iterador_campeonatos = r.db(self.db_name).table('Campeonato').run(self.connection)
		# Itero sobre los campeonatos
		for campeonato in iterador_campeonatos:
			lista_categorias = campeonato.get("ListaCategorias")
			# Para cada campeonato itero sobre sus categorias
			for categoria in lista_categorias:
				# Tomo los ganadadores de medallas
				ganador_oro = (categoria.get("GanadorOro")).get("DNICompetidor")
				ganador_plata = categoria.get("GanadorPlata").get("DNICompetidor")
				ganador_bronce = categoria.get("GanadorBronce").get("DNICompetidor")
				# Esto es una referencia
				dic_aux =dic_de_dic__modalidad[categoria.get("Modalidad")]
				if ganador_oro in dic_aux:
					dic_aux[ganador_oro] += 1
				else:
					dic_aux[ganador_oro] = 1
				if ganador_plata in dic_aux:
					dic_aux[ganador_plata] += 1
				else:
					dic_aux[ganador_plata] = 1
				if ganador_bronce in dic_aux:
					dic_aux[ganador_bronce] += 1
				else:
					dic_aux[ganador_bronce] = 1

		dic_resultado = {}
		# Si hay empate entre competidores, solo me da el que aparece primero
		dic_resultado["Formas"] = max(dic_de_dic__modalidad["Formas"], key=dic_de_dic__modalidad["Formas"].get)
		dic_resultado["Combate"] = max(dic_de_dic__modalidad["Combate"], key=dic_de_dic__modalidad["Combate"].get)
		dic_resultado["Rotura"] = max(dic_de_dic__modalidad["Rotura"], key=dic_de_dic__modalidad["Rotura"].get)

		iterador_campeonatos.close()
		return dic_resultado
