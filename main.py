import csv
import sqlite3
from dataclasses import dataclass, fields
from operator import index
from typing import final
import numpy as np

## METHODS
@dataclass
class method:
    # Input tables that will be used in the simulation
    # Default tables
    veh_mass        : str = 'veh_mass'
    veh_mats        : str = 'veh_material'
    veh_fuel        : str = 'veh_fuel_consumption'
    veh_bats        : str = 'veh_battery'
    flt_vint        : str = 'histFLTvint'
    flt_mode        : int = 0 # 0 = flt_market, 1 = size x tech
    flt_market      : str = 'in_flt_pas'
    size_market     : str = 'in_size_pas'
    tech_market     : str = 'in_tech_pas'
    flt_sr          : str = 'flt_sr'
    flt_use         : str = 'flt_vkt'
    man_n_ef        : str = 'veh_manufacture'
    man_x_ef        : str = 'veh_scrap'
    mat_p_ef        : str = 'in_mat_primary'
    mat_s_ef        : str = 'in_mat_secondary'
    bat_market      : str = 'in_bat_ncx'
    bat_chem        : str = 'bat_chem'
    bat_density     : str = 'bat_density'
    bat_mode        : int = 0 # 0 = manufacturing driven, 1 = amount limit
    batmat_limit    : str = 'in_batmat_limit'
    batman_n_ef     : str = 'in_bat_manufacture_sps'
    batman_x_ef     : str = 'in_bat_scrap_sps'
    batmat_p_ef     : str = 'in_bat_primary_sps'
    batmat_s_ef     : str = 'in_bat_pyro_sps'
    grid_mix        : str = 'in_grid_mix_simple'
    grid_p_ef       : str = 'grid_FES'
    fuel_mode       : int = 0 # 0 = % market, 1 = amount limit
    fuel_market     : str = 'fuel_market_regular'
    fuel_limit      : str = 'in_efuel_limit'
    fuel_p_ef       : str = 'in_fuel_production'
    fuel_u_ef       : str = 'fuel_combustion'
    results         : int = 1 # on/off toggle
    lca_results     : str = 'FLAME-test' # root file name
    veh_results     : int = 1 # on/off toggle
    flt_results     : int = 1 # on/off toggle
    mkt_output      : int = 0 # on/off toggle
    bat_results     : int = 1 # on/off toggle
    batmat_results  : int = 1 # on/off toggle
    nrg_results     : int = 1 # on/off toggle

    def header(self):
        output = []
        f = fields(self)
        for v in f:
            output.append(v.name)
        return output
    
    def values(self):
        output = []
        f = fields(self)
        for v in f:
            output.append(getattr(self,v.name))
        return output


## MODULES
class LCA:
    # Functions related to calculating the annual GHG emissions
    def FLAME(meth:method):
        # The simulation
        LCA.init_all()
        Man.ef()
        Mat.ef()
        Bat.ef()
        Nrg.ef()
        Veh.project()
        if meth.bat_mode == 0 :
            Flt.project()
            Flt.demand()
        elif meth.bat_mode == 1 :
            Flt.init()
            Flt.age(2020)
            Flt.scrap(2020)
            Bat.recycle(2020)
            Bat.allowance(2020)
            for year in sim_years:
                Flt.age(year)
                Flt.scrap(year)
                Bat.recycle(year)
                Bat.allowance(year)
                Flt.frombats(year)
                Flt.salestostock(year)
            Flt.demand()
            # Man.demand()
            # Mat.demand()
            # Nrg.demand()
        LCA.fleet_sim()
        LCA.results()
        print("Simulation complete. ")

    def init_all():
        # Initialise the final results table and module emissions factors and intermediate results tables
        lca_db.executescript("""
            CREATE TABLE IF NOT EXISTS
                "LCAResult" (
                    "Year"	    INTEGER NOT NULL,
                    "Module"	TEXT NOT NULL,
                    "Phase"	    TEXT NOT NULL,
                    "Process"	TEXT NOT NULL,
                    "GHG"	    INTEGER NOT NULL
                )
            ;
            DELETE FROM LCAResult ;
            """)
        LCA.init_efd('MANn','t_id')
        LCA.init_efd('MANx','t_id')
        LCA.init_efd('MATp','m_id')
        LCA.init_efd('MATs','m_id')
        LCA.init_efd('BATMANn','e_id')
        LCA.init_efd('BATMANx','e_id')
        LCA.init_efd('BATMATp','m_id')
        LCA.init_efd('BATMATs','m_id')
        LCA.init_efd('NRGp','f_id')
        LCA.init_efd('NRGu','f_id')
        LCA.init_efd('NRGg','f_id')
        
    def init_efd(table:str,ref:str):
         lca_db.executescript("""
            CREATE TABLE IF NOT EXISTS
                \""""+table+"""EF\" (
                    "Year"	        INTEGER NOT NULL,
                    \""""+ref+"""\"	INTEGER NOT NULL,
                    "ef"	        REAL NOT NULL,
                    PRIMARY KEY ("Year",\""""+ref+"""\")
                )
            ;
            DELETE FROM \""""+table+"""EF\" ;
            CREATE TABLE IF NOT EXISTS
                \""""+table+"""Demand\" (
                    "Year"	        INTEGER NOT NULL,
                    \""""+ref+"""\"	INTEGER NOT NULL,
                    "amount"	        REAL NOT NULL,
                    PRIMARY KEY ("Year",\""""+ref+"""\")
                )
            ;
            DELETE FROM \""""+table+"""Demand\" ;
            """)
        
    def fleet_sim():
        # Calculate all modules annual GHG emissions
        LCA.sim('MAN','Manufacturing','MANnDemand','MANnEF','t_id','refVEHtech')
        LCA.sim('MAN','Scrap','MANxDemand','MANxEF','t_id','refVEHtech')
        LCA.sim('MAT','Primary','MATpDemand','MATpEF','m_id','refMAT')
        LCA.sim('MAT','Secondary','MATsDemand','MATsEF','m_id','refMAT')
        LCA.sim('BATMAN','Manufacturing','BATMANnDemand','BATMANnEF','e_id','refBAT')
        LCA.sim('BATMAN','Scrap','BATMANxDemand','BATMANxEF','e_id','refBAT')
        LCA.sim('BATMAT','Primary','BATMATpDemand','MATpEF','m_id','refMAT')
        LCA.sim('BATMAT','Secondary','BATMATsDemand','MATsEF','m_id','refMAT')
        LCA.sim('NRG','Production','NRGpDemand','NRGpEF','f_id','refNRG')
        LCA.sim('NRG','Use','NRGuDemand','NRGuEF','f_id','refNRG')
        lca_db.execute("""DELETE FROM LCAResult WHERE GHG = 0;""")

    def sim(module:str,phase:str,demand:str,ef:str,id:str,ref:str):
        # Calculate the annual GHG emissions  for the input module
        lca_db.execute("""
            INSERT INTO
                LCAResult
            SELECT
                x.Year
                ,\""""+module+"""\" AS Module
                ,\""""+phase+"""\" AS Phase
                ,z.Reference AS Process
                ,x.amount * y.ef AS GHG
            FROM
                """+demand+""" x
            JOIN
                """+ef+""" y
            USING
                (Year,"""+id+""")
            JOIN
                """+ref+""" z
            USING
                ("""+id+""")
            ;""")

    def results():
        # Output module data to CSV files
        LCA.lca_result()
        if meth.veh_results:
            LCA.veh_result()
        if meth.flt_results:
            LCA.flt_result()
        if meth.mkt_output:
            LCA.mkt_output()
        if meth.bat_results:
            LCA.bat_result()
        if meth.batmat_results:
            LCA.batmat_result()
        if meth.nrg_results:
            LCA.nrg_result()

    def lca_result():
        # Write LCAResults table data to csv file
        lca_file:str = "Outputs/" + meth.lca_results + "-LCCA-" + str(sim_n).zfill(2) + ".CSV"
        lca_result = lca_db.cursor().execute("SELECT * FROM LCAResult;")
        with open(lca_file,"w",newline="") as output_file:
            csv_out = csv.writer(output_file)
            csv_out.writerow(meth.header())
            csv_out.writerow(meth.values())
            csv_out.writerow("")
            csv_out.writerow([d[0] for d in lca_result.description])
            for row in lca_result:
                csv_out.writerow(row)

    def veh_result():
        # Write summary of Vehicle material emissions to csv file
        # Single Vehicle
        veh_file:str = "Outputs/" + meth.lca_results + "-VEH-" + str(sim_n).zfill(2) + ".CSV"
        veh_result = lca_db.cursor().execute("""
            SELECT 
                a.Year
                ,t.\"Reference\" AS Tech
                ,m.\"Reference\" AS Mat
                ,mass*mass_proportion*p.ef*(n.amount/(n.amount+x.amount)) AS \"Primary\"
                ,mass*mass_proportion*s.ef*(x.amount/(n.amount+x.amount)) AS \"Secondary\"
            FROM (SELECT Year, t_id, SUM(sales) AS sales FROM FLTvint GROUP BY Year,t_id) a
            JOIN VEHmaterial USING (Year,t_id)
            JOIN VEHmass USING (Year,t_id)
            JOIN MATpDemand n USING (Year, m_id)
            JOIN MATsDemand x USING (Year, m_id)
            JOIN MATpEF p USING (Year,m_id)
            JOIN MATsEF s USING (Year,m_id)
            JOIN refVEHTech t USING (t_id)
            JOIN refMAT m USING (m_id)
            WHERE mass_proportion IS NOT 0 AND a.Year IN (2020,2025,2030,2035,2040,2045,2050)
            ;""")
        with open(veh_file,"w",newline="") as output_file:
            csv_out = csv.writer(output_file)
            csv_out.writerow(meth.header())
            csv_out.writerow(meth.values())
            csv_out.writerow("")
            csv_out.writerow([d[0] for d in veh_result.description])
            for row in veh_result:
                csv_out.writerow(row)

    def flt_result():
        # Write Fleet data to csv file
        # Fleet Projection
        flt_file:str = "Outputs/" + meth.lca_results + "-FLT-" + str(sim_n).zfill(2) + ".CSV"
        flt_result = lca_db.cursor().execute("""
            SELECT
                Year
                ,\"Reference\" AS Tech
                ,SUM(sales) AS Sales
                ,SUM(stock) AS Stock
                ,SUM(scrap) AS Scrap
            FROM FLTvint
            JOIN refVEHTech USING (t_id)
            GROUP BY Year,t_id
            ;""") # Simple
        with open(flt_file,"w",newline="") as output_file:
            csv_out = csv.writer(output_file)
            csv_out.writerow(meth.header())
            csv_out.writerow(meth.values())
            csv_out.writerow("")
            csv_out.writerow([d[0] for d in flt_result.description])
            for row in flt_result:
                csv_out.writerow(row)
                
    def bat_result():
        # Write Battery manufacturing data to csv file
        # Installed Battery Capacity
        flt_file:str = "Outputs/" + meth.lca_results + "-CAP-" + str(sim_n).zfill(2) + ".CSV"
        flt_result = lca_db.cursor().execute("""
            SELECT
                Year
                ,\"Reference\" AS Chem
                ,n.amount AS Production
                ,x.amount AS Scrap
            FROM BATMANnDemand n
            JOIN BATMANxDemand x USING (Year, e_id)
            JOIN refBAT USING (e_id)
            ;""")
        with open(flt_file,"w",newline="") as output_file:
            csv_out = csv.writer(output_file)
            csv_out.writerow(meth.header())
            csv_out.writerow(meth.values())
            csv_out.writerow("")
            csv_out.writerow([d[0] for d in flt_result.description])
            for row in flt_result:
                csv_out.writerow(row)

    def batmat_result():
        # Write Battery materials data to csv file
        # Battery Materials
        bat_file:str = "Outputs/" + meth.lca_results + "-BAT-" + str(sim_n).zfill(2) + ".CSV"
        bat_result = lca_db.cursor().execute("""
                                             SELECT 
                                                Year
                                                ,\"Desc\" AS Mat
                                                ,p.amount AS \"Primary\"
                                                ,s.amount AS \"Secondary\" 
                                             FROM BATMATpDemand p
                                             JOIN BATMATsDemand s USING (Year,m_id)
                                             JOIN refMAT USING (m_id)
                                             ;""")
        with open(bat_file,'w',newline="") as output_file:
            csv_out = csv.writer(output_file)
            csv_out.writerow(meth.header())
            csv_out.writerow(meth.values())
            csv_out.writerow("")
            csv_out.writerow([d[0] for d in bat_result.description])
            for row in bat_result:
                csv_out.writerow(row)

    def nrg_result():
        # Write energy demands to csv file
        nrg_file:str = "Outputs/" + meth.lca_results + "-NRG-" + str(sim_n).zfill(2) + ".CSV"
        nrg_result = lca_db.cursor().execute("""
                                             SELECT
                                                Year
                                                ,\"Reference\" AS Fuel
                                                , amount AS Amount
                                             FROM NRGpDemand
                                             JOIN refNRG USING (f_id)
                                             ;""")
        with open(nrg_file,'w',newline="") as output_file:
            csv_out = csv.writer(output_file)
            csv_out.writerow(meth.header())
            csv_out.writerow(meth.values())
            csv_out.writerow("")
            csv_out.writerow([d[0] for d in nrg_result.description])
            for row in nrg_result:
                csv_out.writerow(row)

class Veh:
    # Functions related to projecting vehicle parameters
    def project():
        # Update vehicle parameters
        Veh.init()
        # Parameters accounding to module methods
        annualise(meth.veh_mass,'VEHmass',['t_id','mass'])
        annualise(meth.veh_mats,'VEHmaterial',['t_id','m_id','mass_proportion'])
        annualise(meth.veh_fuel,'VEHfuel',['t_id','f_id','fuel_consumption','utility_factor'])
        annualise(meth.veh_bats,'VEHbat',['t_id','battery_capacity'])

    def init():
        # Initialise the vheicle parameter tables
        lca_db.executescript("""
            CREATE TABLE IF NOT EXISTS
                "VEHmass" (
                    "Year" INTEGAR NOT NULL
                    ,"t_id" INTEGAR NOT NULL
                    ,"mass" REAL NOT NULL
                    ,PRIMARY KEY ("Year","t_id")
                )
            ;
            DELETE FROM
                "VEHmass"
            ;
            CREATE TABLE IF NOT EXISTS
                "VEHmaterial" (
                    "Year" INTEGAR NOT NULL
                    ,"t_id" INTEGAR NOT NULL
                    ,"m_id" INTEGAR NOT NULL
                    ,"mass_proportion" REAL NOT NULL
                    ,PRIMARY KEY ("Year","t_id","m_id")
                )
            ;
            DELETE FROM
                "VEHmaterial"
            ;
            CREATE TABLE IF NOT EXISTS
                "VEHfuel" (
                    "Year" INTEGAR NOT NULL
                    ,"t_id" INTEGAR NOT NULL
                    ,"f_id" INTEGAR NOT NULL
                    ,"fuel_consumption" REAL NOT NULL
                    ,"utility_factor" REAL NOT NULL
                    ,PRIMARY KEY ("Year","t_id","f_id")
                )
            ;
            DELETE FROM
                "VEHfuel"
            ;
            CREATE TABLE IF NOT EXISTS
                "VEHbat" (
                    "Year" INTEGAR NOT NULL
                    ,"t_id" INTEGAR NOT NULL
                    ,"battery_capacity" REAL NOT NULL
                    ,PRIMARY KEY ("Year","t_id")
                )
            ;
            DELETE FROM
                "VEHbat"
            ;""")

class Flt:
    # Funcation related to fleet turnover
    def project():
        # Calcualte fleet vintage table using market method
        Flt.init()
        # Simulate fleet
        for year in sim_years:
            Flt.age(year)
            Flt.market(year)
            Flt.scrap(year)
            Flt.stocktosales(year)

    def init():
        # Initialise vintage table using historical year
        lca_db.executescript("""
            CREATE TABLE IF NOT EXISTS
                "FLTvint" (
                    "Year" INTEGER NOT NULL
                    ,"t_id" INTEGER NOT NULL DEFAULT 0
                    ,"age" INTEGER NOT NULL
                    ,"sales" INTEGER NOT NULL DEFAULT 0
                    ,"stock" INTEGER NOT NULL DEFAULT 0
                    ,"scrap" INTEGER NOT NULL DEFAULT 0
                    ,PRIMARY KEY ("Year","t_id","age")
                )
            ;
            DELETE FROM
                "FLTvint"
            ;
            INSERT INTO
                FLTvint
            SELECT
                Year
                ,t_id
                ,age
                ,sales
                ,stock
                ,scrap
            FROM
                """+meth.flt_vint+"""
            WHERE
                Year = 2020
            ;
            CREATE TABLE IF NOT EXISTS
                "FLTmarket" (
                    "Year"	        INTEGER NOT NULL,
                    "t_id"	INTEGER NOT NULL,
                    "market_share"	        REAL NOT NULL,
                    PRIMARY KEY ("Year","t_id")
                )
            ;
            DELETE FROM
                "FLTmarket"
            ;
            CREATE TABLE IF NOT EXISTS
                "FLTsr" (
                    "Year"  INTEGER NOT NULL
                    ,"t_id"	INTEGER NOT NULL DEFAULT 0
                    ,"age"	INTEGER NOT NULL DEFAULT 0.0
                    ,"survival_rate"	REAL NOT NULL
                    ,PRIMARY KEY("Year","t_id","age")
                )
            ;
            DELETE FROM
                "FLTsr"
            ;
            CREATE TABLE IF NOT EXISTS
                "FLTvkt" (
                    "Year"  INTEGER NOT NULL
                    ,"t_id"	INTEGER NOT NULL DEFAULT 0
                    ,"vkt"	INTEGER NOT NULL DEFAULT 0.0
                    ,PRIMARY KEY("Year","t_id")
                )
            ;
            DELETE FROM
                "FLTvkt"
            ;""") # year BETWEEN 2020 AND 2022
        annualise(meth.flt_market,'FLTmarket',['t_id','market_share'])
        annualise(meth.flt_sr,'FLTsr',['t_id','age','survival_rate'])
        annualise(meth.flt_use,'FLTvkt',['t_id','vkt'])

    def age(sim_year:int):
        # Age the fleet by one year, scrap vehicles using survival rate
        lca_db.execute("""
            INSERT INTO
                FLTvint
            SELECT
                :sim_year as Year
                ,a.t_id
                ,a.age + 1 AS age
                ,0 AS sales
                ,CAST ( ROUND ( a.stock * b.survival_rate ) AS INT ) AS stock
                ,0 AS scrap
            FROM
                FLTvint a
            JOIN
                FLTSR b
            ON
                b.Year = a.Year
            AND
                b.t_id = a.t_id
            AND
                b.age = MIN(a.age,30)
            WHERE
                a.Year = :sim_year - 1
            ;"""
            ,{"sim_year":sim_year})

    def market(sim_year:int):
        # Calculate Age(0) fleet using population and market share
        # Insert F(a=0)
        lca_db.execute("""
            INSERT INTO
                FLTvint
            SELECT
                a.Year
                ,a.t_id
                ,0 as age
                ,0 as sales
                ,CAST ( ROUND ( ( (b.pop * b.roo) - d.fleet ) * c.market_share ) AS INT) as stock
                ,0 as scrap
            FROM
                FLTvint a
            JOIN
                flt_population b
            USING
                (Year)
            JOIN
                FLTmarket c
            USING
                (Year,t_id)
            JOIN
                (SELECT Year,SUM(stock) AS fleet FROM FLTvint GROUP BY Year ) d
            USING
                (Year)
            WHERE
                a.Year = :sim_year
            GROUP BY
                a.t_id
            ;"""
            ,{"sim_year":sim_year})

    def frombats(sim_year:int):
        # Sell as many xEVs as fits in the limit, in the ratio target by the fleet market.  Excess vehicle demand is made up by P-ICEV and D-ICEV from the backup market.
        # This might be ridiculous
        lca_db.execute("""
            INSERT INTO
                FLTvint
            SELECT
                Year
                ,t_id
                ,0 AS age
                ,CAST ( ROUND ( MIN ( battery_limit/battery_capacity*market_share/ev_market , ((pop*roo-fleet)*market_share)/survival_rate ) ) AS INT ) AS sales
                ,0 AS stock
                ,0 AS scrap
            FROM (SELECT Year,SUM(amount_limit) AS battery_limit FROM BATMANnLimit GROUP BY Year)
            JOIN VEHbat USING (Year)
            JOIN FLTmarket USING (Year,t_id)
            JOIN (SELECT Year,SUM(market_share) AS ev_market FROM FLTmarket WHERE t_id IN (SELECT DISTINCT t_id FROM VEHbat) GROUP BY Year) USING (Year)
            JOIN flt_population USING (Year)
            JOIN (SELECT Year,SUM(stock) AS fleet FROM FLTvint GROUP BY Year) USING (Year)
            JOIN FLTsr USING (Year,t_id)
            WHERE Year = :sim_year AND age = 0
            ;"""
            ,{"sim_year":sim_year})
        pass
        ##### here:
        # lca_db.execute("""
        #     INSERT INTO
        #         FLTvint
        #     SELECT
        #         Year
        #         ,t_id
        #         ,0 as age
        #         ,CAST ( ROUND (  ( (pop*roo)-(fleet+ev_fleet) ) ) AS INT ) as sales
        #         ,0 AS stock
        #         ,0 AS scrap
        #     FROM FLTmarket
        #     JOIN flt_population USING (Year)
        #     JOIN (SELECT Year,SUM(stock) AS fleet FROM FLTvint GROUP BY Year) USING (Year)
        #     JOIN (SELECT Year,SUM(sales*survival_rate) AS ev_fleet FROM FLTvint JOIN FLTsr USING (Year,t_id,age) GROUP BY Year) USING (Year)
        #     WHERE
        #         Year = :sim_year
        #     AND NOT t_id IN (SELECT DISTINCT t_id FROM VEHbat)
        #     ;"""
        #     ,{"sim_year":sim_year})

    def scrap(sim_year:int):
        # Update scrap numbers
        lca_db.execute("""
            UPDATE
                FLTvint
            SET
                scrap = old.stock - FLTvint.stock
            FROM
                FLTvint old
            WHERE
                old.Year = FLTvint.Year - 1
            AND
                old.age = FLTvint.age - 1
            AND
                old.t_id = FLTvint.t_id
            AND
                FLTvint.Year = :sim_year
            ;"""
            ,{"sim_year":sim_year})

    def stocktosales(sim_year:int):
        # Sales and scrap from F(a=0)
        lca_db.execute("""
            UPDATE
                FLTvint
            SET
                sales = CAST ( ROUND ( FLTvint.stock / SR.survival_rate ) AS INT )
                ,scrap = CAST ( ROUND ( FLTvint.stock * (1.0 - SR.survival_rate) / SR.survival_rate ) AS INT )
            FROM
                flt_sr SR
            WHERE
                FLTvint.Year = :sim_year
            AND
                FLTvint.t_id = SR.t_id
            AND
                FLTvint.age = 0
            AND
                SR.age = MIN(FLTvint.age,30)
            ;"""
            ,{"sim_year":sim_year}) # flt_sr

    def salestostock(sim_year:int):
        # Stock and scrap from N
        lca_db.execute("""
            UPDATE
                FLTvint
            SET
                stock = (CAST (ROUND(FLTvint.sales * SR.survival_rate) AS INT))
                ,scrap = (CAST (ROUND(FLTvint.sales * (1 - SR.survival_rate)) AS INT))
            FROM
                flt_sr SR
            WHERE
                FLTvint.Year = :sim_year
            AND
                FLTvint.t_id = SR.t_id
            AND
                FLTvint.age = 0
            AND
                SR.age = MIN(FLTvint.age,30)
            ;"""
            ,{"sim_year":sim_year}) # flt_sr

    def demand():
        # Calculate module demands
        Man.demand()
        Mat.demand()
        Bat.demand()
        Nrg.demand()

class Man:
    # Funcation related to vehicle manufacture
    def demand():
        # Manufacturing demand is one per new vehicle
        # only tech; add size?
        # only age = 0; to allow for imports as "sales"?
        lca_db.executescript("""
            INSERT INTO
                MANnDemand
            SELECT
                a.Year
                ,a.t_id
                ,SUM( a.sales ) AS "amount"
            FROM
                FLTvint a
            GROUP BY
                a.Year,a.t_id
            ;
            INSERT INTO
                MANxDemand
            SELECT
                a.Year
                ,a.t_id
                ,SUM( a.scrap ) AS "amount"
            FROM
                FLTvint a
            GROUP BY
                a.Year,a.t_id
            ;""")

    def ef():
        # Emission factor according to module method
        annualise(meth.man_n_ef,'MANnEF',['t_id','ef'])
        annualise(meth.man_x_ef,'MANxEF',['t_id','ef'])

class Mat:
    # Functions related to bulk material use
    def demand():
        # Material demand for new vehicles
        lca_db.executescript("""
            INSERT INTO
                MATsDemand
            SELECT
                a.Year
                ,b.m_id
                ,SUM( a.scrap * c.mass * b.mass_proportion * d.recovery_rate ) AS amount
            FROM
                FLTvint a
            JOIN
                VEHmaterial b
            USING
                (Year,t_id)
            JOIN
                VEHmass c
            USING
                (Year,t_id)
            JOIN
                MATsRR d
            USING
                (Year,m_id)
            GROUP BY
                a.Year,b.m_id
            ;
            INSERT INTO
                MATpDemand
            SELECT
                a.Year
                ,b.m_id
                ,MAX(0, SUM( a.sales * c.mass * b.mass_proportion ) - d.amount) AS amount
            FROM
                FLTvint a
            JOIN
                VEHmaterial b
            USING
                (Year,t_id)
            JOIN
                VEHmass c
            USING
                (Year,t_id)
            JOIN
                MATsDemand d
            USING
                (Year,m_id)
            GROUP BY
                a.Year,b.m_id
            ;
            """)
        # Actual recycling + recovery rates?
        # Everything open loop recycling

    def ef():
        # Emission factor according to module method
        annualise(meth.mat_p_ef,'MATpEF',['m_id','ef'])
        annualise(meth.mat_s_ef,'MATsEF',['m_id','ef'])
        lca_db.executescript("""
            CREATE TABLE IF NOT EXISTS
                "MATsRR" (
                    "Year"	        INTEGER NOT NULL,
                    "m_id"	INTEGER NOT NULL,
                    "recovery_rate"	        REAL NOT NULL,
                    PRIMARY KEY ("Year","m_id")
                )
            ;
            DELETE FROM "MATsRR" ;
            """)
        annualise(meth.mat_s_ef,'MATsRR',['m_id','recovery_rate'])

class Bat:
    # Functions related to traction battery materials and manufacture
    def demand():
        # Calculate battery materials and manufacture demand
        Bat.man_demand()
        Bat.mat_demand()

    def man_demand():
        # Battery manufacturing demand per kWh produced and scrapped
        if meth.bat_mode == 0 :
            lca_db.executescript("""
                INSERT INTO
                    BATMANnDemand
                SELECT
                    a.Year
                    ,c.e_id
                    ,SUM( a.sales * b.battery_capacity * c.market_share ) AS "amount"
                FROM
                    FLTvint a
                JOIN
                    VEHbat b
                USING
                    (Year,t_id)
                JOIN
                    BATmarket c
                USING
                    (Year)
                GROUP BY
                    a.Year,c.e_id
                ;
                INSERT INTO
                    BATMANxDemand
                SELECT
                    a.Year
                    ,c.e_id
                    ,SUM( a.scrap * b.battery_capacity * c.market_share ) AS "amount"
                FROM
                    FLTvint a
                JOIN
                    VEHbat b
                USING
                    (Year,t_id)
                JOIN
                    BATmarket c
                ON
                    c.Year = MAX(2020, a.Year - a.age)
                GROUP BY
                    a.Year,c.e_id
                ;""")
        elif meth.bat_mode == 1 :
            lca_db.executescript("""
                INSERT INTO
                    BATMANnDemand
                SELECT
                    a.Year
                    ,c.e_id
                    ,SUM( a.sales * b.battery_capacity * c.market_share ) AS "amount"
                FROM
                    FLTvint a
                JOIN
                    VEHbat b
                USING
                    (Year,t_id)
                JOIN
                    BATmarket c
                USING
                    (Year)
                GROUP BY
                    a.Year,c.e_id
                ;""")

    def mat_demand():
        # Critical battery material flow per kWh produced and scrapped with closed-loop recycling
        if meth.bat_mode == 0 :
            lca_db.executescript("""
                INSERT INTO
                    BATMATsDemand
                SELECT
                    a.Year
                    ,b.m_id
                    ,SUM( a.amount * b.mass_proportion * c.mass_per_kWh * d.recovery_rate ) AS amount
                FROM
                    BATMANxDemand a
                JOIN
                    BATchem b
                USING
                    (Year,e_id)
                JOIN
                    BATDensity c
                USING
                    (Year,e_id)
                JOIN
                    BATMATsRR d
                USING
                    (Year,m_id)
                GROUP BY
                    a.Year,b.m_id
                ;
                INSERT INTO
                    BATMATpDemand
                SELECT
                    a.Year
                    ,b.m_id
                    ,MAX(0, SUM( a.amount * b.mass_proportion * c.mass_per_kWh ) - d.amount) AS amount
                FROM
                    BATMANnDemand a
                JOIN
                    BATchem b
                USING
                    (Year,e_id)
                JOIN
                    BATDensity c
                USING
                    (Year,e_id)
                JOIN
                    BATMATsDemand d
                USING
                    (Year,m_id)
                GROUP BY
                    a.Year,b.m_id
                ;
                """)
        elif meth.bat_mode == 1 :
            lca_db.executescript("""
                INSERT INTO
                    BATMATpDemand
                SELECT
                    a.Year
                    ,b.m_id
                    ,MAX(0, SUM( a.amount * b.mass_proportion * c.mass_per_kWh ) - d.amount) AS amount
                FROM
                    BATMANnDemand a
                JOIN
                    BATchem b
                USING
                    (Year,e_id)
                JOIN
                    BATDensity c
                USING
                    (Year,e_id)
                JOIN
                    BATMATsDemand d
                USING
                    (Year,m_id)
                GROUP BY
                    a.Year,b.m_id
                ;
                """)

    def allowance(sim_year:int):
        # Define new battery manufacture that fits within primary material limit
        lca_db.execute("""
            INSERT INTO
                BATMANnLimit
            SELECT
                Year
                ,e_id
                ,MIN ((amount_limit + amount) / need ) * market_share AS "amount_limit"
            FROM
                BATMATpLimit
            JOIN
                bat_chem
            USING
                (m_id)
            JOIN
                (SELECT Year,m_id,SUM(mass_proportion*market_share) AS need
                FROM BATmarket
                JOIN bat_chem USING (e_id)
                GROUP BY year,m_id)
            USING
                (year,m_id)
            JOIN
                BATmarket
            USING
                (year, e_id)
            JOIN
                BATMATsDemand
            USING
                (year,m_id)
            WHERE
                Year = :sim_year
            GROUP BY
                year,e_id
            ;"""
            ,{"sim_year":sim_year})

    def recycle(sim_year:int):
        # Recycle EoL vehicle traction batteries for secondary material
        lca_db.execute("""
            INSERT INTO
                BATMANxDemand
            SELECT
                a.Year
                ,c.e_id
                ,SUM( a.scrap * b.battery_capacity * c.market_share ) AS "amount"
            FROM
                FLTvint a
            JOIN
                VEHbat b
            USING
                (Year,t_id)
            JOIN
                BATmarket c
            ON
                c.Year = MAX(2020, a.Year - a.age)
            WHERE
                a.Year = :sim_year
            GROUP BY
                a.Year,c.e_id
            ;"""
            ,{"sim_year":sim_year})
        lca_db.execute("""
            INSERT INTO
                BATMATsDemand
            SELECT
                a.Year
                ,b.m_id
                ,SUM( a.amount * b.mass_proportion * c.mass_per_kWh * d.recovery_rate ) AS amount
            FROM
                BATMANxDemand a
            JOIN
                BATchem b
            USING
                (Year,e_id)
            JOIN
                BATDensity c
            USING
                (Year,e_id)
            JOIN
                BATMATsRR d
            USING
                (Year,m_id)
            WHERE
                a.Year = :sim_year
            GROUP BY
                a.Year,b.m_id
            ;"""
            ,{"sim_year":sim_year})

    def ef():
        # Emission factors according to module methods
        annualise(meth.batman_n_ef,'BATMANnEF',['e_id','ef'])
        annualise(meth.batman_x_ef,'BATMANxEF',['e_id','ef'])
        annualise(meth.batmat_p_ef,'MATpEF',['m_id','ef'])
        annualise(meth.batmat_s_ef,'MATsEF',['m_id','ef'])
        lca_db.executescript("""
            CREATE TABLE IF NOT EXISTS
                "BATmarket" (
                    "Year"	        INTEGER NOT NULL,
                    "e_id"	INTEGER NOT NULL,
                    "market_share"	        REAL NOT NULL,
                    PRIMARY KEY ("Year","e_id")
                )
            ;
            DELETE FROM "BATmarket" ;
            """)
        annualise(meth.bat_market,'BATmarket',['e_id','market_share'])
        lca_db.executescript("""
            CREATE TABLE IF NOT EXISTS
                "BATchem" (
                    "Year"	        INTEGER NOT NULL,
                    "e_id"	INTEGER NOT NULL,
                    "m_id"	INTEGER NOT NULL,
                    "mass_proportion"	        REAL NOT NULL,
                    PRIMARY KEY ("Year","e_id","m_id")
                )
            ;
            DELETE FROM "BATchem" ;
            """)
        annualise(meth.bat_chem,'BATchem',['e_id','m_id','mass_proportion'])
        lca_db.executescript("""
            CREATE TABLE IF NOT EXISTS
                "BATdensity" (
                    "Year"	        INTEGER NOT NULL,
                    "e_id"	INTEGER NOT NULL,
                    "mass_per_kWh"	        REAL NOT NULL,
                    PRIMARY KEY ("Year","e_id")
                )
            ;
            DELETE FROM "BATdensity" ;
            """)
        annualise(meth.bat_density,'BATdensity',['e_id','mass_per_kWh'])
        lca_db.executescript("""
            CREATE TABLE IF NOT EXISTS
                "BATMATsRR" (
                    "Year"	        INTEGER NOT NULL,
                    "m_id"	INTEGER NOT NULL,
                    "recovery_rate"	        REAL NOT NULL,
                    PRIMARY KEY ("Year","m_id")
                )
            ;
            DELETE FROM "BATMATsRR" ;
            """)
        annualise(meth.batmat_s_ef,'BATMATsRR',['m_id','recovery_rate'])
        if meth.bat_mode == 1 :
            lca_db.executescript("""
                CREATE TABLE IF NOT EXISTS
                    "BATMATpLimit" (
                        "Year"	        INTEGER NOT NULL,
                        "m_id"	INTEGER NOT NULL,
                        "amount_limit"	        REAL NOT NULL,
                        PRIMARY KEY ("Year","m_id")
                    )
                ;
                DELETE FROM "BATMATpLimit" ;
                """)
            annualise(meth.batmat_limit,'BATMATpLimit',['m_id','amount_limit'])
            lca_db.executescript("""
                CREATE TABLE IF NOT EXISTS
                    "BATMANnLimit" (
                        "Year"	        INTEGER NOT NULL,
                        "e_id"	INTEGER NOT NULL,
                        "amount_limit"	        REAL NOT NULL,
                        PRIMARY KEY ("Year","e_id")
                    )
                ;
                DELETE FROM "BATMANnLimit" ;
                """)

class Nrg:
    # Functions related to fleet operations, fuel use and energy generation
    def demand():
        # Total demand for fuel or energy from average annual VKT
        if meth.fuel_mode == 0 :
            # Alternative fuels as a proportion of the total market
            lca_db.executescript("""
                INSERT INTO
                    NRGpDemand
                SELECT
                    a.Year
                    ,d.x_id
                    ,SUM( 1.0 * a.fleet * b.fuel_consumption * b.utility_factor * c.vkt * d.market_share / 100.0 ) AS amount
                FROM
                    (SELECT
                        Year
                        ,t_id
                        ,SUM( stock ) AS fleet
                    FROM
                        FLTvint
                    GROUP BY
                        Year,t_id
                    ) a
                JOIN
                    VEHfuel b
                USING
                    (Year,t_id)
                JOIN
                    FLTvkt c
                USING
                    (Year,t_id)
                JOIN
                    NRGmarket d
                USING
                    (Year,f_id)
                GROUP BY
                    a.Year,d.x_id
                ;
                INSERT INTO
                    NRGuDemand
                SELECT
                    Year
                    ,f_id
                    ,amount
                FROM
                    NRGpDemand
                ;""")
        elif meth.fuel_mode == 1 :
            # Up to a total limit of alternative fuel availability
            lca_db.executescript("""
                INSERT INTO
                    NRGpDemand
                SELECT
                    a.Year
                    ,b.f_id
                    ,SUM( 1.0 * a.fleet * b.fuel_consumption * b.utility_factor * c.vkt / 100.0 ) AS amount
                FROM
                    (SELECT
                        Year
                        ,t_id
                        ,SUM( stock ) AS fleet
                    FROM
                        FLTvint
                    GROUP BY
                        Year,t_id
                    ) a
                JOIN
                    VEHfuel b
                USING
                    (Year,t_id)
                JOIN
                    FLTvkt c
                USING
                    (Year,t_id)
                GROUP BY
                    a.Year,b.f_id
                ;
                INSERT INTO
                    NRGpDemand
                SELECT
                    a.Year
                    ,b.x_id
                    ,IIF(a.amount >= c.alt_limit, b.amount_limit
                        ,IIF(a.amount < c.alt_limit, 1.0*a.amount*b.amount_limit/c.alt_limit, 0)
                    )
                FROM
                    NRGpDemand a
                JOIN
                    NRGlimit b
                USING
                    (Year,f_id)
                JOIN
                    (SELECT Year,f_id,SUM(amount_limit) as alt_limit FROM NRGlimit GROUP BY Year,f_id) c
                USING
                    (Year,f_id)
                ;
                UPDATE
                    NRGpDemand
                SET
                    amount = NRGpDemand.amount - a.alt_amount
                FROM
                    (SELECT
                        x.Year
                        ,y.f_id
                        ,SUM(x.amount) AS alt_amount
                    FROM
                        NRGpDemand x
                    JOIN
                        NRGlimit y
                    ON
                        x.Year=y.Year
                    AND
                        x.f_id = y.x_id
                    GROUP BY
                        x.Year,y.x_id
                    ) a
                WHERE
                    a.Year = NRGpDemand.Year
                AND
                    a.f_id = NRGpDemand.f_id
                ;  
                INSERT INTO
                    NRGuDemand
                SELECT
                    Year
                    ,f_id
                    ,amount
                FROM
                    NRGpDemand
                ;""")
        else :
            print("NRG demand error: fuel_mode unknown.")

    def ef():
        # Emission factor according to module method
        annualise(meth.fuel_p_ef,'NRGpEF',['f_id','ef'])
        annualise(meth.fuel_u_ef,'NRGuEF',['f_id','ef'])
        annualise(meth.grid_p_ef,'NRGgEF',['f_id','ef'])
        # Grid generation emission factor
        lca_db.executescript("""
            CREATE TABLE IF NOT EXISTS
                "NRGmix" (
                    "Year"	        INTEGER NOT NULL,
                    "f_id"	INTEGER NOT NULL,
                    "grid_mix"	        REAL NOT NULL,
                    PRIMARY KEY ("Year","f_id")
                )
            ;
            DELETE FROM
                "NRGmix"
            ;""")
        annualise(meth.grid_mix, 'NRGmix' ,['f_id','grid_mix'])
        lca_db.executescript("""
            INSERT INTO
                NRGpEF
            SELECT
                Year
                ,6 as f_id
                ,SUM ( grid_mix * ef ) as ef
            FROM
                NRGgEF a
            JOIN
                NRGmix b
            USING
                (Year,f_id)
            GROUP BY
                Year
            ;""")
        if meth.fuel_mode == 0 :
            lca_db.executescript("""
                CREATE TABLE IF NOT EXISTS
                    "NRGmarket" (
                        "Year"	        INTEGER NOT NULL,
                        "f_id"	INTEGER NOT NULL,
                        "x_id"	INTEGER NOT NULL,
                        "market_share"	        REAL NOT NULL,
                        PRIMARY KEY ("Year","f_id","x_id")
                    )
                ;
                DELETE FROM
                    "NRGmarket"
                ;""")
            annualise(meth.fuel_market,'NRGmarket',['f_id','x_id','market_share'])
        elif meth.fuel_mode == 1 :
            lca_db.executescript("""
                CREATE TABLE IF NOT EXISTS
                    "NRGlimit" (
                        "Year"	        INTEGER NOT NULL,
                        "f_id"	INTEGER NOT NULL,
                        "x_id"	INTEGER NOT NULL,
                        "amount_limit"	        REAL NOT NULL,
                        PRIMARY KEY ("Year","f_id","x_id")
                    )
                ;
                DELETE FROM
                    "NRGlimit"
                ;""")
            annualise(meth.fuel_limit,'NRGlimit',['f_id','x_id','amount_limit'])
        else :
            print("NRG ef error: fuel_mode unknown.")


## GLOBAL FUNCTIONS
def table_exists(table:str):
    # Check if input table exists in the database
    query = lca_db.execute("""
        SELECT
            1
        FROM
            sqlite_master
        WHERE
            type = \"table\"
        AND
            name = \""""+table+"""\"
        ;""").fetchone()[0]
    return query is not None

def method_type(method_table:str):
    # Returns true if module method input table is defined with time
    query = lca_db.execute("""
        SELECT CASE
            WHEN
                EXISTS (
                    SELECT
                        1
                    FROM
                        pragma_table_info(\""""+method_table+"""\")
                    WHERE
                        name LIKE \"year\"
                )
            THEN
                1
            ELSE
                0
            END
        ;""").fetchone()[0]
    return query

def annualise(source:str,target:str,ref:list):
    # Input the module emission factor table into the module table
    refs = ""
    for r in ref:
        refs += ",\""+r+"\""
    if method_type(source) == 1:
        lca_db.execute("""
            INSERT INTO
                \""""+target+"""\"
            SELECT
                Year
                """+refs+"""
            FROM
                \""""+source+"""\"
            ;""")
    else:
        for sim_year in list(range(2020,2051,1)):
            lca_db.execute("""
                INSERT INTO
                    \""""+target+"""\"
                SELECT
                    :sim_year AS Year
                    """+refs+"""
                FROM
                    \""""+source+"""\"
                ;"""
                ,{"sim_year":sim_year})

def fixed(source:str,target:str,ref:list):
    refs = ""
    for r in ref:
        refs += ",\""+r+"\""
    lca_db.execute("""
        INSERT INTO
            \""""+target+"""\"
        SELECT
            """+refs+"""
        FROM
            \""""+source+"""\"
        ;""")


## OPEN DATABASE
lca_db = sqlite3.connect('UK-FLAME.db')


## GLOBAL VARIABLES
sim_years = list(range(2021,2051,1)) # set max range, go dynamic by historical values?
sim_n = 0


## SIMULATION
# METHODS
FLAMEmethods = list()

## ELECTRIFICATION
FLAMEmethods.append(method(lca_results='FLAME-BatMat')) #01
FLAMEmethods.append(method(flt_market='in_flt_delay',lca_results='FLAME-BatMat')) #02
FLAMEmethods.append(method(batmat_s_ef='in_bat_hydro_sps',lca_results='FLAME-BatMat')) #03
FLAMEmethods.append(method(bat_market='in_bat_LFP',batmat_s_ef='in_bat_hydro_sps',lca_results='FLAME-BatMat')) #04
FLAMEmethods.append(method(bat_market='in_bat_LFP',flt_market='in_flt_delay',batmat_s_ef='in_bat_hydro_sps',lca_results='FLAME-BatMat')) #05
FLAMEmethods.append(method(grid_p_ef='grid_IEA',lca_results='FLAME-BatMat')) #06

## MXC
FLAMEmethods.append(method(flt_market='in_flt_early'  ,lca_results='FLAME-MXC')) #01
FLAMEmethods.append(method(flt_market='in_flt_early'  ,grid_p_ef='grid_flat',lca_results='FLAME-MXC')) #02
FLAMEmethods.append(method(flt_market='in_flt_BEV2035',lca_results='FLAME-MXC')) #03
FLAMEmethods.append(method(flt_market='in_flt_BEV2035',grid_p_ef='grid_flat',lca_results='FLAME-MXC')) #04
FLAMEmethods.append(method(flt_market='in_flt_BEV2045',lca_results='FLAME-MXC')) #05
FLAMEmethods.append(method(flt_market='in_flt_BEV2045',grid_p_ef='grid_flat',lca_results='FLAME-MXC')) #06
FLAMEmethods.append(method(flt_market='in_flt_xp'     ,lca_results='FLAME-MXC')) #07
FLAMEmethods.append(method(flt_market='in_flt_xp'     ,grid_p_ef='grid_flat',lca_results='FLAME-MXC')) #08
FLAMEmethods.append(method(flt_market='in_flt_fixed'  ,lca_results='FLAME-MXC')) #09
FLAMEmethods.append(method(flt_market='in_flt_fixed'  ,grid_p_ef='grid_flat',lca_results='FLAME-MXC')) #10


# THE SIMULATION
for meth in FLAMEmethods:
    meth:method
    sim_n += 1
    print(sim_n)
    # print(meth) # degbug
    LCA.FLAME(meth)


## CLOSE DATABASE
lca_db.commit()
lca_db.close()