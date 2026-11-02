import sys,time,re
import asyncio
from typing import Any
from abc import ABC, abstractmethod


class Logable:
    """
    Classe de base fournissant un système de journalisation.

    Les messages essentiels sont toujours écrits.
    Les messages détaillés ne sont écrits que si verbose=True.
    """

    logfile = None

    @classmethod
    def set_logfile(cls, logfile) -> None:
        cls.logfile = logfile

    def __init__(
            self, 
            name: str, 
            verbose: bool, 
        ) -> None:
        self.name = name
        self.verbose = verbose

    def log(self, msg: str) -> None:
        """
        Écrit un message dans le fichier de log.

        detail=False :
            message essentiel, toujours affiché.

        detail=True :
            message détaillé, affiché seulement si verbose=True.
        """
        if not self.verbose:
            return

        if Logable.logfile is None:
            print(f"[{self.name}] {msg}")
        else:
            print(
                f"[{self.name}] {msg}",
                file=Logable.logfile,
                flush=True
            )


class Accessoire(Logable):
    """
    Classe de base des accessoires asynchrones.
    """

    def __init__(
        self,
        name: str,
        verbose: bool,
        queue: asyncio.Queue[Any] | None = None
    ) -> None:
        super().__init__(
            name=name,
            verbose=verbose,
        )

        self._queue: asyncio.Queue[Any] = (
            queue 
            if queue is not None 
            else asyncio.Queue()
        )

    async def enqueue(self, item: Any) -> None:
        await self._queue.put(item)

    async def dequeue(self) -> Any:
        return await self._queue.get()

    def empty(self) -> bool:
        return self._queue.empty()

    def qsize(self) -> int:
        return self._queue.qsize()

    
class Pic(Accessoire):
    """ 
    Un pic contient les commandes à fabriquer.

    Il peut embrocher un post-it contenant une commande
    par-dessus les post-it déjà présents, puis libérer
    le dernier post-it embroché.

    Le fonctionnement est donc de type LIFO :
    Last In, First Out.
    """

    def __init__(self, name: str, verbose: bool) -> None:
        super().__init__(
            name=name,
            verbose=verbose,
            queue=asyncio.LifoQueue()
        )

    async def embrocher(self, postit) -> None:
        """
        Ajoute un post-it au sommet du pic.
        """
        await self.enqueue(postit)

        self.log(
            f"post-it '{postit}' embroché, "
            f"{self.qsize()} post-it(s) à traiter"
        )

    async def liberer(self):
        """
        Attend qu'un post-it soit disponible,
        puis retourne le dernier post-it embroché.
        """
        postit = await self.dequeue()
        
        self.log(
            f"post-it '{postit}' libéré, "
            f"{self.qsize()} post-it(s) à traiter"
        )

        return postit

    
class Bar(Accessoire):
    """ 
    Un bar peut recevoir des commandes composées de consommations,
    en attente d'être servies.

    Le fonctionnement est de type FIFO :
    First In, First Out.
    """

    def __init__(
            self, 
            name: str, 
            verbose: bool,
        ) -> None:
        super().__init__(
            name=name,
            verbose=verbose,
            queue=asyncio.Queue()
        )

    async def recevoir(self, commande) -> None:
        """
        Dépose une commande prête sur le bar.
        """
        await self.enqueue(commande)

        self.log(
            f"'{commande}' posée, "
            f"{self.qsize()} commande(s) à servir",
        )

    async def evacuer(self):
        """
        Attend qu'une commande soit disponible,
        puis retourne la plus ancienne commande déposée.
        """
        commande = await self.dequeue()
        
        self.log(
            f"'{commande}' évacuée, "
            f"{self.qsize()} commande(s) à servir",
        )
        
        return commande


class Employe(Logable):
    """
    Classe de base modélisant un employé.
    """

    def __init__(
        self,
        pic,
        bar,
        clients,
        name: str,
        verbose: bool,
    ) -> None:

        super().__init__(
            name=name,
            verbose=verbose,
        )

        self.pic = pic
        self.bar = bar
        self.clients = clients

        self.log("prêt pour le service")

    
class Serveur(Employe):
    def __init__(
            self, 
            pic: Pic, 
            bar: Bar, 
            clients: 'Clients',
            name: str,
            verbose: bool,
            ):
        
        super().__init__(
            pic=pic,
            bar=bar,
            clients=clients,
            name=name,
            verbose=verbose,
        )

    async def prendre_commande(self) -> None:
        
        while True:
            commande = await self.clients.commande()

            if commande is None:
                await self.pic.embrocher(None)
                break

            self.log(
                f"j'ai la commande '{commande}'"
            )

            self.log(
                f"j'écris sur le post-it '{commande}'"
            )

            await self.pic.embrocher(commande)

    async def servir(self):
        while True:
                commande = await self.bar.evacuer()
                
                if commande is None:
                    break
                
                self.log(
                    f"j'apporte la commande '{commande}'"
                )

                for conso in commande:
                    self.log(
                        f"je sers '{conso}'"
                    )

    async def run(self):
        await asyncio.gather(
            self.prendre_commande(),
            self.servir()
        )

class Barman(Employe):
    def __init__(
            self, 
            pic: Pic, 
            bar: Bar, 
            clients: 'Clients',
            name: str,
            verbose: bool
        ):
        super().__init__(
            pic=pic,
            bar=bar,
            clients=clients,
            name=name,
            verbose=verbose
        )


    async def preparer(self):
        while True:
            commande = await self.pic.liberer()

            if commande is None:
                await self.bar.recevoir(None)
                break

            self.log(
                f"je commence la fabrication de '{commande}'"
            )

            for conso in commande:
                self.log(
                    f"je prépare '{conso}'"
                )

            await self.bar.recevoir(commande)

            self.log(
                f"la commande '{commande}' est prête"
            )

    async def run(self):
        await self.preparer()


class Clients:
    """
    Représente les clients et les commandes planifiées dans un fichier.

    Chaque ligne du fichier contient :
        <temporisation> <liste de consommations>

    Exemple :
        4 bloody mary,moscow mule
    """

    def __init__(self, fname: str):
        self.commandes = []

        fmt = re.compile(r"(\d+)\s+(.*)")

        instant = time.monotonic()

        with open(fname, "r", encoding="utf-8") as fichier:
            for ligne in fichier:
                found = fmt.search(ligne)

                if found:
                    delai = int(found.group(1))
                    contenu = found.group(2)

                    # Le délai est relatif à la commande précédente
                    instant += delai

                    commande = [
                        consommation.strip()
                        for consommation in contenu.split(",")
                    ]

                    self.commandes.append((instant, commande))


    async def commande(self):
        """
        Attend jusqu'à l'instant prévu de la prochaine commande.

        Retourne None lorsqu'il n'y a plus de commandes.
        """

        if not self.commandes:
            return None

        instant, commande = self.commandes.pop(0)

        attente = instant - time.monotonic()

        if attente > 0:
            await asyncio.sleep(attente)

        return commande



# ============================================================
# MAIN
# ============================================================

async def main(
    alice: Serveur,
    bob: Barman
) -> None:

    await asyncio.gather(
        alice.run(),
        bob.run()
    )

def usage() -> None:
    print(
        f"usage: {sys.argv[0]} fichier"
    )

    sys.exit(1)


if __name__ == "__main__":

    if len(sys.argv) != 2:
        usage()

    fcommandes = sys.argv[1]

    logfile = "/tmp/borabora.log"

    print(
        f"logging in {logfile}..."
    )

    with open(
        logfile,
        "a",
        encoding="utf-8"
    ) as logf:

        Logable.set_logfile(logf)

        print(
            "\n--- nouvelle exécution ---",
            file=logf,
            flush=True
        )

        les_clients = Clients(
            fcommandes
        )

        le_pic = Pic(
            name="Pic",
            verbose=True,
        )

        le_bar = Bar(
            name="Bar",
            verbose=True,
        )

        bob = Barman(
            pic=le_pic,
            bar=le_bar,
            clients=les_clients,
            name="Bob",
            verbose=True,
        )

        alice = Serveur(
            pic=le_pic,
            bar=le_bar,
            clients=les_clients,
            name="Alice",
            verbose=True,
        )

        asyncio.run(
            main(alice, bob)
        )