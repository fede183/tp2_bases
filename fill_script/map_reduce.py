#!/usr/bin/python
# -*- coding: utf-8 -*-

from db_manager import DBmanager
import rethinkdb as r
from random import randint

'''
Javascript implementation: (Solo testeada a mano en la base de datos)
r.db('Itf_TKD_WC_historico').table('Competidor')
    .map(function(c) { return {
        NombreEscuela: c("NombreEscuela"),
        cantidad_medallas: c("Oro").count().add(c("Bronce").count()).add(c("Plata").count())
    }})
    .group('NombreEscuela')
    .reduce(function(left, right) {
        return { cantidad_medallas: left("cantidad_medallas").add(right("cantidad_medallas"))
    }})
'''


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class MapReduce:

    def __init__(self, db_name, connection):
        print (bcolors.HEADER + 'Consulta implementada: 2.2 La cantidad de' +
               ' medallas por nombre de escuela en toda la historia' + bcolors.ENDC)
        self.db_name = db_name
        self.connection = connection

    def map_function(self, esc):
        return {
            'medallas': esc['Oro'].count()
            .add(esc['Bronce'].count())
            .add(esc['Plata'].count())
        }

    def reduce_function(self, right, left):
        return {
            'medallas': right['medallas'].add(left['medallas'])
        }

    def run_map_reduce(self):
        return r.db(self.db_name) \
                .table('Competidor') \
                .group('NombreEscuela') \
                .map(lambda doc: self.map_function(doc)) \
                .reduce(lambda l, r: self.reduce_function(l, r)) \
                .run(self.connection)

    def run_test_1(self, cant_comp=10):
        print (bcolors.UNDERLINE + 'En este test se crean ' + str(cant_comp) + ' competidores todos con 3 medallas y' +
               'asignados a escuelas distintas')
        print 'El resultado esperado es 3 medallas por escuela', bcolors.ENDC
        original_db_name = self.db_name

        self.db_name = 'test_map_reduce'
        db_test = DBmanager(self.db_name)
        db_test.create_db(override=True)

        for i in range(cant_comp):
            db_test.insert_competidor(i, 'COMP_' + str(i), 'ESC_' + str(i), [1], [1], [1])

        res_map_reduce = self.run_map_reduce()

        for k, v in res_map_reduce.iteritems():
            if v['medallas'] == 3:
                print bcolors.OKGREEN, k + 'OK', bcolors.ENDC
            else:
                print bcolors.WARNING, k, 'Wrong', 'Deberia ser 3 y es ' + str(['medallas']), bcolors.ENDC

        self.db_name = original_db_name

    def run_test_2(self, cant_escuelas=10, cant_comp_escuela=10):
        print (bcolors.BOLD + 'En este test se crean ' + str(cant_escuelas) +
               ' escuelas cada una con ' + str(cant_comp_escuela) +
               ' competidores todos con 3 medallas')
        print 'El resultado esperado es ' + str(3 * cant_comp_escuela) + ' medallas por escuela', bcolors.ENDC
        original_db_name = self.db_name

        self.db_name = 'test_map_reduce'
        db_test = DBmanager(self.db_name)
        db_test.create_db(override=True)

        dni = 0
        for i in range(cant_escuelas):
            esc = 'ESC_' + str(i)
            for j in range(cant_comp_escuela):
                db_test.insert_competidor(dni, 'COMP_' + str(dni), esc, [1], [1], [1])
                dni += 1

        res_map_reduce = self.run_map_reduce()

        for k, v in res_map_reduce.iteritems():
            if v['medallas'] == 3 * cant_comp_escuela:
                print bcolors.OKGREEN, k, 'OK', bcolors.ENDC
            else:
                print (bcolors.WARNING + k + 'Wrong' +
                       'Deberia ser ' + str(3 * cant_comp_escuela) + ' y es ' + str(v['medallas']) + bcolors.ENDC)

        self.db_name = original_db_name

    def run_test_3(self, cant_escuelas=10, cant_max_comp_escuela=10):
        print (bcolors.UNDERLINE + 'En este test se crean ' + str(cant_escuelas) +
               ' escuelas cada una con una cantidad random entre 0 y ' +
               str(cant_max_comp_escuela) +
               ' competidores todos con 3 medallas')
        print 'El resultado esperado es cant_comp_escuela_i * ' + str(3) + ' medallas para cada escuela_i', bcolors.ENDC
        original_db_name = self.db_name

        self.db_name = 'test_map_reduce'
        db_test = DBmanager(self.db_name)
        db_test.create_db(override=True)

        dni = 0
        cant_per_escuela = {}
        for i in range(cant_escuelas):
            esc = 'ESC_' + str(i)
            cant_per_escuela[esc] = randint(0, cant_max_comp_escuela)
            for j in range(cant_per_escuela[esc]):
                db_test.insert_competidor(dni, 'COMP_' + str(dni), esc, [1], [1], [1])
                dni += 1

        res_map_reduce = self.run_map_reduce()

        for k, v in res_map_reduce.iteritems():
            if v['medallas'] == 3 * cant_per_escuela[k]:
                print bcolors.OKGREEN, k, v['medallas'], 'OK', bcolors.ENDC
            else:
                print (bcolors.WARNING + k + 'Wrong: Deberia ser ' + str(3 * cant_per_escuela[k]) +
                       ' y es ' + str(v['medallas']))

        self.db_name = original_db_name

    def run_all_test(self, p1=10, p2=10, p3=10, p4=10, p5=10):
        self.run_test_1(p1)
        self.run_test_2(p2, p3)
        self.run_test_3(p4, p5)


if __name__ == '__main__':
    data_base = DBmanager()

    m = MapReduce(data_base.db_name, data_base.connection)
    m.run_all_test()
    print bcolors.OKBLUE, 'Corriendo consulta map reduce en la base de datos', bcolors.ENDC
    for k, v in m.run_map_reduce().iteritems():
        print k, ':', v['medallas']
