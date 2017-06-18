#!/usr/bin/python
# -*- coding: utf-8 -*-

import rethinkdb as r
from rethinkdb.errors import *


class TKDDBgenerator:

    def __init__(self):
        self.connection = r.connect("localhost", 28015)
        self.db_name = 'Itf_TKD_WC_historico'
        self.table_infos = {
            'Arbitro': {'name': 'Arbitro', 'pk': 'DNIArbitro'},
            'Categoria': {'name': 'Categoria', 'pk': 'idCategoria'},
            'Escuela': {'name': 'Escuela', 'pk': 'NombreEscuela'},
            'Campeonato': {'name': 'Campeonato', 'pk': 'yearCampeonato'},
            'Enfrentamiento': {'name': 'Enfrentamiento', 'pk': 'idEnfrentamiento'},
            'Competidor': {'name': 'Competidor', 'pk': 'DNICompetidor'},
        }

    def create_db(self, override=False):
        try:
            print('Creando base de datos: ', self.db_name)
            r.db_create(self.db_name).run(self.connection)
        except ReqlOpFailedError:
            if override:
                print('La base de datos: ', self.db_name, ' ya existe, sobre-escibiendo')
                self.drop_db()
                print('Creando base de datos: ', self.db_name)
                r.db_create(self.db_name).run(self.connection)
            else:
                print('La base de datos ya existe, si desea forzar la sobreescritura, use el parametro override=True')
                return None

        for table in self.table_infos.itervalues():
            print('Creando tabla: ', table['name'])
            r.db(self.db_name).table_create(table['name'], primary_key=table['pk']).run(self.connection)
        print('Base de datos: ', self.db_name, ' Creada')

    def empty_table(self, table_name):

        if table_name in self.table_infos.keys():
            table = self.table_infos[table_name]
            try:
                r.db(self.db_name).table_drop(table['name']).run(self.connection)
                r.db(self.db_name).table_create(table['name'], primary_key=table['pk']).run(self.connection)
            except ReqlOpFailedError:
                print('La tabla ' + tabla['name'] + ' no existe')
        else:
            print('La tabla especificada no forma parte de la base de datos')

    def drop_db(self):
        try:
            print('Eliminando la base de datos: ', self.db_name)
            r.db_drop(self.db_name).run(self.connection)
            print('Base de datos: ', self.db_name, ' Eliminada')
        except ReqlOpFailedError:
            print('La base de datos: ', self.db_name, ' No existe')

    def insert(self, table_name, register):
        r.db(self.db_name) \
            .table(table_name) \
            .insert(register) \
            .run(self.connection)
        print('Insertado correctamente: ', register, table_name)

    def append(self, table_name, pk_value, attribute, value):
        if table_name in self.table_infos.keys():
            table = self.table_infos[table_name]
            r.db(self.db_name) \
                .table(table['name']) \
                .filter(r.row[table['pk']] == pk_value) \
                .update({attribute: r.row[attribute].append(value)}) \
                .run(self.connection)
        else:
            print('La tabla especificada no forma parte de la base de datos')

    def update(self, table_name, pk_value, attribute, value):
        if table_name in self.table_infos.keys():
            table = self.table_infos[table_name]
            r.db(self.db_name) \
                .table(table['name']) \
                .filter(r.row[table['pk']] == pk_value) \
                .update({attribute: value}) \
                .run(self.connection)
        else:
            print('La tabla especificada no forma parte de la base de datos')

    # Specific table insert update append methods

    def insert_arbitro(self, DNIArbitro, NombreArbitro, ListaCampeonatos=[]):
        new_document = {
            'DNIArbitro': DNIArbitro,
            'NombreArbitro': NombreArbitro,
            'ListaCampeonatos': ListaCampeonatos
        }
        self.insert(self.table_infos['Arbitro']['name'], new_document)

    def add_campeonato_to_arbitro(self, DNIArbitro, yearCampeonato):
        self.append('Arbitro', DNIArbitro, 'ListaCampeonatos', yearCampeonato)

    def insert_categoria(self, idCategoria, Modalidad,
                         GanadorOro={}, GanadorPlata={}, GanadorBronce={}):
        new_document = {
            'idCategoria': idCategoria,
            'Modalidad': Modalidad,
            'GanadorOro': GanadorOro,
            'GanadorPlata': GanadorPlata,
            'GanadorBronce': GanadorBronce
        }
        self.insert(self.table_infos['Categoria']['name'], new_document)

    def update_medalla_from_categoria(self, idCategoria, medalla, DNICompetidor, NombreEscuela):
        new_document = {
            'DNICompetidor': DNICompetidor,
            'NombreEscuela': NombreEscuela
        }
        self.update('Categoria', idCategoria, medalla, new_document)

    def insert_escuela(self, NombreEscuela, ListaCompetidores=[], ListaCampeonatos=[]):
        new_document = {
            'NombreEscuela': NombreEscuela,
            'ListaCompetidores': ListaCompetidores,
            'ListaCampeonatos': ListaCampeonatos
        }
        self.insert(self.table_infos['Escuela']['name'], new_document)

    def add_competidor_to_escuela(self, NombreEscuela, DNICompetidor, Oro, Plata, Bronce):
        new_document = {
            'DNICompetidor': DNICompetidor,
            'Oro': Oro,
            'Plata': Bronce,
            'Bronce': Plata
        }
        self.append('Escuela', NombreEscuela, 'ListaCompetidores', new_document)

    def add_campeonato_to_escuela(self, NombreEscuela, yearCampeonato):
        self.append('Escuela', NombreEscuela, 'ListaCampeonatos', yearCampeonato)

    def insert_campeonato(self, yearCampeonato, ListaCompetidores=[], ListaCategorias=[],
                          ListaEscuelas=[], ListaArbitros=[], ListaEnfrentamientos=[]):
        new_document = {
            'yearCampeonato': yearCampeonato,
            'ListaCompetidores': ListaCompetidores,
            'ListaCategorias': ListaCategorias,
            'ListaEscuelas': ListaEscuelas,
            'ListaArbitros': ListaArbitros,
            'ListaEnfrentamientos': ListaEnfrentamientos
        }
        self.insert(self.table_infos['Campeonato']['name'], new_document)

    def add_competidor_to_campeonato(self, yearCampeonato, DNICompetidor, NombreEscuela):
        new_document = {
            'DNICompetidor': DNICompetidor,
            'NombreEscuela': NombreEscuela
        }
        self.append('Campeonato', yearCampeonato, 'ListaCompetidores', new_document)

    def add_categoria_to_campeonato(self, yearCampeonato, idCategoria,
                                    Modalidad, GanadorOro, GanadorPlata, GanadorBronce):
        new_document = {
            'idCategoria': idCategoria,
            'Modalidad': Modalidad,
            'GanadorOro': GanadorOro,
            'GanadorPlata': GanadorPlata,
            'GanadorBronce': GanadorBronce
        }
        self.append('Campeonato', yearCampeonato, 'ListaCategorias', new_document)

    def add_escuela_to_campeonato(self, yearCampeonato, NombreEscuela):
        self.append('Campeonato', yearCampeonato, 'ListaEscuelas', NombreEscuela)

    def add_arbitro_to_campeonato(self, yearCampeonato, DNIArbitro):
        self.append('Campeonato', yearCampeonato, 'ListaArbitros', DNIArbitro)

    def add_enfrentamiento_to_campeonato(self, yearCampeonato, idEnfrentamiento):
        self.append('Campeonato', yearCampeonato, 'ListaEnfrentamientos', idEnfrentamiento)

    def insert_enfrentamiento(self, idEnfrentamiento, yearCampeonato):
        new_document = {
            'idEnfrentamiento': idEnfrentamiento,
            'yearCampeonato': yearCampeonato
        }
        self.insert(self.table_infos['Enfrentamiento']['name'], new_document)

    def insert_competidor(self, DNICompetidor, NombreCompetidor, NombreEscuela,
                          EnfrentamientosPerdidos=[], EnfrentamientosGanados=[], Oro=[], Plata=[], Bronce=[]):
        new_document = {
            'DNICompetidor': DNICompetidor,
            'NombreCompetidor': NombreCompetidor,
            'NombreEscuela': NombreEscuela,
            'EnfrentamientosPerdidos': EnfrentamientosPerdidos,
            'EnfrentamientosGanados': EnfrentamientosGanados,
            'Oro': Oro,
            'Plata': Plata,
            'Bronce': Bronce
        }
        self.insert(self.table_infos['Competidor']['name'], new_document)

    def add_medalla_to_competidor(self, DNICompetidor, medalla, idCategoria):
        self.append('Competidor', DNICompetidor, medalla, idCategoria)

    def add_enfrentamiento_ganado_to_competidor(self, DNICompetidor, idEnfrentamiento):
        self.append('Competidor', DNICompetidor, 'EnfrentamientosGanados', idEnfrentamiento)

    def add_enfrentamiento_perdido_to_competidor(self, DNICompetidor, idEnfrentamiento):
        self.append('Competidor', DNICompetidor, 'EnfrentamientosPerdidos', idEnfrentamiento)
