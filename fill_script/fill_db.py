#!/usr/bin/python
# -*- coding: utf-8 -*-
from db_manager import DBmanager
import pandas as pd
from random import randint


class DBfill:

    def __init__(self, cant_years, cant_arbitros, competidores_por_escuela, enfrentamientos_por_competidor):
        self.d = DBmanager()
        self.cant_years = cant_years
        self.cant_arbitros = cant_arbitros
        self.competidores_por_escuela = competidores_por_escuela
        self.enfrentamientos_por_competidor = enfrentamientos_por_competidor
        self.nextId = 0
        self.nextDNI = 30000000

    def load_db(self):
        # Este metodo resetea la base de datos
        # El orden en que se cargan las tablas es importante
        self.d.create_db(override=True)
        self.load_campeonato()
        self.load_arbitros()
        self.load_escuelas()
        self.load_competidores()
        self.load_categorias()
        self.load_enfrentamientos()

    def getNewId(self):
        self.nextId += 1
        return self.nextId

    def getNewDNI(self):
        self.nextDNI += 1
        return self.nextDNI

    def load_campeonato(self):
        print('Cargando campeonatos')
        for y in range(self.cant_years):
            self.d.insert_campeonato(2017 - y)

    def load_arbitros(self):
        print('Cargando arbitros')
        nombres = pd.read_csv('../csv/nombres.csv', sep=',')
        campeonatos = self.d.query('Campeonato').items
        for a in range(self.cant_arbitros):
            dni = self.getNewDNI()
            self.d.insert_arbitro(dni, nombres['Nombre'][randint(0, len(nombres) - 1)])
            for c in campeonatos:
                self.d.add_campeonato_to_arbitro(dni, c['yearCampeonato'])

    def load_escuelas(self):
        print('Cargando Escuelas...')
        escuelas = pd.read_csv('../csv/escuelas.csv', sep=',')
        for e in escuelas.iterrows():
            self.d.insert_escuela(e[1]['Nombre'])

    def load_competidores(self):
        print('Cargando Competidores...')
        nombres = pd.read_csv('../csv/nombres.csv', sep=',')
        escuelas = self.d.query('Escuela').items
        campeonatos = self.d.query('Campeonato').items
        for e in escuelas:
            for n in range(self.competidores_por_escuela):
                dni = self.getNewDNI()
                nombre = nombres['Nombre'][randint(0, len(nombres) - 1)]
                self.d.insert_competidor(dni, nombre, e['NombreEscuela'])
                self.d.add_competidor_to_escuela(e['NombreEscuela'], dni)
                for c in campeonatos:
                    self.d.add_competidor_to_campeonato(c['yearCampeonato'], dni, e['NombreEscuela'])

            for c in campeonatos:
                self.d.add_escuela_to_campeonato(c['yearCampeonato'], e['NombreEscuela'])
                self.d.add_campeonato_to_escuela(e['NombreEscuela'], c['yearCampeonato'])

    def load_categorias(self):
        categorias = pd.read_csv('../csv/categorias.csv', sep=',')
        competidores = self.d.query('Competidor').items
        campeonatos = self.d.query('Campeonato').items
        for campeonato in campeonatos:
            for c in categorias.iterrows():
                idCategoria = self.getNewId()
                print('Insertando categoria: ', c[1]['Nombre'])
                indexes = range(len(competidores))
                index = randint(0, len(indexes) - 1)
                oro = {
                    'DNICompetidor': competidores[index]['DNICompetidor'],
                    'NombreEscuela': competidores[index]['NombreEscuela']
                }
                self.d.add_medalla_to_competidor(competidores[index]['DNICompetidor'], 'Oro', idCategoria)
                self.d.add_medalla_to_competidor_in_escuela(competidores[index]['NombreEscuela'],
                                                            competidores[index]['DNICompetidor'], 'Oro')
                indexes.pop(index)
                index = randint(0, len(indexes) - 1)
                bronce = {
                    'DNICompetidor': competidores[index]['DNICompetidor'],
                    'NombreEscuela': competidores[index]['NombreEscuela']
                }
                self.d.add_medalla_to_competidor(competidores[index]['DNICompetidor'], 'Bronce', idCategoria)
                self.d.add_medalla_to_competidor_in_escuela(competidores[index]['NombreEscuela'],
                                                            competidores[index]['DNICompetidor'], 'Bronce')
                indexes.pop(index)
                index = randint(0, len(indexes) - 1)
                plata = {
                    'DNICompetidor': competidores[index]['DNICompetidor'],
                    'NombreEscuela': competidores[index]['NombreEscuela']
                }
                self.d.add_medalla_to_competidor(competidores[index]['DNICompetidor'], 'Plata', idCategoria)
                self.d.add_medalla_to_competidor_in_escuela(competidores[index]['NombreEscuela'],
                                                            competidores[index]['DNICompetidor'], 'Plata')
                self.d.add_categoria_to_campeonato(campeonato['yearCampeonato'], idCategoria, c[1]['Nombre'],
                                                   oro, plata, bronce)

    def load_enfrentamientos(self):
        campeonatos = self.d.query('Campeonato').items
        competidores = self.d.query('Competidor').items
        for campeonato in campeonatos:
            for i in range(len(competidores)):
                first = i + self.competidores_por_escuela
                last = i + self.competidores_por_escuela + self.enfrentamientos_por_competidor
                for j in range(first, min(last, len(competidores))):
                    if randint(0, 1) == 1:  # Gana o pierde de forma random uniforme
                        self.d.insert_enfrentamiento(self.getNewId(), campeonato['yearCampeonato'],
                                                     competidores[i]['DNICompetidor'], competidores[j]['DNICompetidor'])
                    else:
                        self.d.insert_enfrentamiento(self.getNewId(), campeonato['yearCampeonato'],
                                                     competidores[j]['DNICompetidor'], competidores[i]['DNICompetidor'])


if __name__ == '__main__':
    f = DBfill(3, 10, 10, 2)
    f.load_db()
