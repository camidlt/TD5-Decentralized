import bodyParser from "body-parser";
import express from "express";
import { BASE_NODE_PORT } from "../config";
import { NodeState, Value } from "../types";
import { delay } from "../utils";

// Type pour les messages échangés entre les nœuds
type Message = {
  type: "R" | "P"; // R pour Round 1, P pour Round 2
  nodeId: number;
  k: number; // numéro du round
  value: Value;
};

// Paramètres ajustables pour les délais
const MAX_WAIT_TIME = 20; // temps maximum (en ms) d'attente pour recevoir les messages d'un round
const POLLING_INTERVAL = 5; // intervalle (en ms) entre deux vérifications

// Si à true, le tie-breaker utilisera une décision aléatoire (Math.random()), sinon il sera déterministe (k % 2)
const USE_RANDOM_TIEBREAKER = false;

export async function node(
  nodeId: number,
  N: number,
  F: number,
  initialValue: Value,
  isFaulty: boolean,
  nodesAreReady: () => boolean,
  setNodeIsReady: (index: number) => void
) {
  const app = express();
  app.use(express.json());
  app.use(bodyParser.json());

  // État du nœud
  const state: NodeState = {
    killed: false,
    x: isFaulty ? null : initialValue,
    decided: isFaulty ? null : false,
    k: isFaulty ? null : 1,
  };

  // Stockage des messages reçus par round
  const roundMessages: { [round: number]: Message[] } = {};
  let isRunning = false;

  // Seuil de tolérance des fautes (ici, pour les tests, on considère F <= floor((N-1)/2))
  const threshold = Math.floor((N - 1) / 2);


  app.get("/status", (req, res) => {
    if (isFaulty) {
      return res.status(500).send("faulty");
    }
    return res.status(200).send("live");
  });

  app.post("/message", (req, res) => {
    if (isFaulty || state.killed || state.decided === true) {
      return res.status(200).send("OK");
    }
    const message = req.body as Message;
    if (!roundMessages[message.k]) {
      roundMessages[message.k] = [];
    }
    roundMessages[message.k].push(message);
    return res.status(200).send("OK");
  });

  app.get("/start", async (req, res) => {
    if (isFaulty || state.killed || isRunning) {
      return res.status(200).send("OK");
    }
    isRunning = true;
    res.status(200).send("OK");
    if (!isFaulty) {
      await runConsensus();
    }
    return;
  });

  app.get("/stop", async (req, res) => {
    state.killed = true;
    isRunning = false;
    res.status(200).send("OK");
  });

  app.get("/getState", (req, res) => {
    res.status(200).json(state);
  });


  async function broadcast(message: Message) {
    if (state.killed) return;
    for (let i = 0; i < N; i++) {
      try {
        await fetch(`http://localhost:${BASE_NODE_PORT + i}/message`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(message),
        });
      } catch {
        // On ignore les erreurs (par exemple, si un nœud défectueux n'est pas joignable)
      }
    }
  }

  async function runConsensus() {
    if (isFaulty || state.killed) return;
    while (!state.decided && !state.killed) {
      if (state.k == null) break;
      const k = state.k;

      // Phase R : On diffuse la valeur actuelle.
      await broadcast({
        type: "R",
        nodeId,
        k,
        value: state.x as Value,
      });

      // Attente des messages de phase R (N - F messages attendus)
      await waitForMessages(k, "R", N - F);
      if (state.killed) break;

      // Récupération des messages de la phase R
      const rValues = collectValues(k, "R");

      // Recherche d'une majorité stricte (au moins Math.floor(N/2)+1 occurrences)
      const majority = findMajority(rValues, Math.floor(N / 2) + 1);

      // Tie-breaker : si aucune majorité, utiliser un choix déterministe ou aléatoire
      let newValue: Value;
      if (majority === null) {
        if (USE_RANDOM_TIEBREAKER) {
          newValue = Math.random() < 0.5 ? 0 : 1;
        } else {
          newValue = (k % 2) as Value;
        }
      } else {
        newValue = majority;
      }

      // Phase P : Diffuser la valeur proposée
      await broadcast({
        type: "P",
        nodeId,
        k,
        value: newValue,
      });

      // Attente des messages de phase P
      await waitForMessages(k, "P", N - F);
      if (state.killed) break;

      const pValues = collectValues(k, "P");

      // Recherche d'une majorité stricte dans les messages P
      const pMajority = findMajority(pValues, Math.floor(N / 2) + 1);

      if (F <= threshold) {
        if (pMajority !== null) {
          state.x = pMajority;
          state.decided = true;
        } else {
          state.x = newValue;
        }
        // Forcer la décision si aucun consensus n'est atteint avant le round 10
        if (!state.decided && k >= 10) {
          state.decided = true;
        }
      } else {
        // Si le nombre de fautes dépasse le seuil toléré, on n'aboutit jamais à une décision
        if (pValues.length > 0) {
          state.x = pValues[0];
        }
        state.decided = false;
      }

      // Passage au round suivant
      state.k = k + 1;

      // Nettoyage des messages du round traité pour éviter toute interférence ultérieure
      delete roundMessages[k];

      // Court délai pour permettre le déroulement rapide des rounds
      await delay(5);
    }
  }

  function collectValues(k: number, msgType: "R" | "P"): Value[] {
    if (!roundMessages[k]) return [];
    return roundMessages[k]
      .filter((m) => m.type === msgType)
      .map((m) => m.value);
  }

  /**
   * Renvoie une valeur (0 ou 1) si une majorité stricte (>= minCount) est atteinte, sinon null.
   */
  function findMajority(values: Value[], minCount: number): Value | null {
    const count0 = values.filter((v) => v === 0).length;
    const count1 = values.filter((v) => v === 1).length;
    if (count0 >= minCount) return 0;
    if (count1 >= minCount) return 1;
    return null;
  }

  /**
   * Attendre jusqu'à maxWaitTime ms pour recevoir au moins `count` messages du type msgType dans le round k.
   */
  async function waitForMessages(
    k: number,
    msgType: "R" | "P",
    count: number
  ): Promise<boolean> {
    const start = Date.now();
    while (Date.now() - start < MAX_WAIT_TIME) {
      if (state.killed) return false;
      const msgs = roundMessages[k] || [];
      const have = msgs.filter((m) => m.type === msgType).length;
      if (have >= count) return true;
      await delay(POLLING_INTERVAL);
    }
    return false;
  }

  const server = app.listen(BASE_NODE_PORT + nodeId, () => {
    console.log(`Node ${nodeId} listening on port ${BASE_NODE_PORT + nodeId}`);
    setNodeIsReady(nodeId);
  });

  return server;
}
