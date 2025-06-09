"""
Prototype retrieval system for matching companies to relevant climate policies.

This script uses an embedding model to index policy descriptions and retrieve
semantically similar matches based on a company’s jurisdiction and sector.
Given the limited (simulated) data, results are reranked using Anthropic’s
Claude model for illustrative purposes.

Originally explored heuristic SQL-based matching, but shifted to vector
similarity due to sparse sectoral data. In a real-world scenario, this would
benefit from richer datasets—such as the 34M-row Climate Policy Radar corpus:
https://huggingface.co/datasets/ClimatePolicyRadar/all-document-text-data

Evaluation should prioritise recall over precision to ensure relevant policies
aren’t missed. Suitable metrics include weighted Precision@K and Recall@K.
"""

import os

import re
import json
import sqlite3
import anthropic
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer


os.environ["TOKENIZERS_PARALLELISM"] = "false"

# You can get an anthropic API key here: https://console.anthropic.com/settings/keys
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
client = anthropic.Client(api_key=ANTHROPIC_API_KEY)

DB_PATH = "data/maiven.db"
EMBED_MODEL = "all-MiniLM-L6-v2"
TOP_K = 10
TOP_N = 3


def load_policies(db_path):
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT id, name, description FROM policy WHERE description <> ''"
    ).fetchall()
    conn.close()
    if not rows:
        return [], [], []

    ids, names, descs = zip(*rows)
    return list(ids), list(names), list(descs)


def load_companies(db_path):
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT company_id, name, operating_jurisdiction, sector FROM company"
    ).fetchall()
    conn.close()
    return [
        {
            "company_id": cid,
            "name": nm,
            "operating_jurisdiction": jur or "",
            "sector": sec or "",
        }
        for cid, nm, jur, sec in rows
    ]


def build_index(descs, model_name=EMBED_MODEL):
    model = SentenceTransformer(model_name)
    embeds = model.encode(descs, convert_to_numpy=True, show_progress_bar=True)
    idx = faiss.IndexFlatL2(embeds.shape[1])
    idx.add(embeds)
    return model, idx


def retrieve_candidates(model, idx, ids, descs, comp, k=TOP_K):
    q = (
        f"Recommend policies for a company operating in "
        f"{comp['operating_jurisdiction']} in the {comp['sector']} sector."
    )
    q_emb = model.encode([q], convert_to_numpy=True)
    _, I = idx.search(q_emb, k)
    return [ids[i] for i in I[0]], [descs[i] for i in I[0]]


def rerank_with_claude(comp, cand_ids, cand_descs):
    prompt = anthropic.HUMAN_PROMPT + (
        "You are a policy assistant.\n"
        f"Company: {comp['name']}\nJurisdiction: {comp['operating_jurisdiction']}\n"
        f"Sector: {comp['sector']}\n\n"
        "Score each policy desc 1–10 and return EXACTLY a JSON array of "
        "{policy_id:int, score:int}, sorted by score desc.\n\n"
    )
    for pid, d in zip(cand_ids, cand_descs):
        prompt += f"ID: {pid}\n{d}\n\n---\n\n"
    prompt += anthropic.AI_PROMPT

    resp = client.completions.create(
        model="claude-2",
        prompt=prompt,
        temperature=0.0,
        max_tokens_to_sample=1024,
    )
    raw = resp.completion.strip()
    try:
        ranked = json.loads(raw)
    except json.JSONDecodeError:
        print(f"[Warning] Non-JSON from Claude: {raw}")
        m = re.search(r"\[.*\]", raw, re.DOTALL)
        if m:
            ranked = json.loads(m.group(0))
        else:
            return []
    ranked.sort(key=lambda x: x["score"], reverse=True)
    return [(o["policy_id"], o["score"]) for o in ranked]


def main():
    policy_ids, policy_names, policy_descs = load_policies(DB_PATH)
    companies = load_companies(DB_PATH)
    model, idx = build_index(policy_descs)

    for comp in companies:
        cand_ids, cand_descs = retrieve_candidates(
            model, idx, policy_ids, policy_descs, comp
        )
        top = rerank_with_claude(comp, cand_ids, cand_descs)[:TOP_N]
        print(f"The top {TOP_N} for {comp['name']} (ID {comp['company_id']})")
        for pid, score in top:
            name = policy_names[policy_ids.index(pid)]
            print(f" • {pid} ({name}): {score}/10")


if __name__ == "__main__":
    main()
