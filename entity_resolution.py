import logging
from collections import defaultdict, deque
from fuzzywuzzy import fuzz
import pandas as pd
import re
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class EntityResolver:
    SIMILARITY_THRESHOLD = 85

    def __init__(self, filepath):
        self.filepath = filepath
        self.data = pd.read_parquet(filepath, engine="pyarrow")
        self.data["website_domain"] = (
            self.data["website_domain"].fillna(
                self.data["website_url"].apply(self.extract_domain))
        )
        self.data["normalized_name"] = (
            self.data["company_name"].apply(self.normalize_name)
        )
        self.possible_duplicates = defaultdict(set)

    @staticmethod
    def extract_domain(url):
        if not isinstance(url, str) or not url.strip():
            return None

        parsed = urlparse(url)
        domain = parsed.netloc.replace("www.", "").strip()

        return domain if domain else None

    @staticmethod
    def clean_text(text):
        return re.sub(r'\W+', '',
                      str(text).lower()) if isinstance(text, str) else ""

    @staticmethod
    def normalize_name(name):
        name = name.lower() if isinstance(name, str) else ""
        patterns = r'\b(inc|ltd|llc|corp|co|company|gmbh|s\.?a\.?|bv|plc)\b'
        name = re.sub(patterns, '', name)
        return re.sub(r'[^a-z0-9]', '', name).strip()

    def find_duplicates(self):
        for col in ["website_domain", "primary_email", "normalized_name"]:
            groups = self.data.groupby(col).groups
            for ids in groups.values():
                ids = list(ids)
                for i in range(len(ids)):
                    for j in range(i + 1, len(ids)):
                        self.possible_duplicates[ids[i]].add(ids[j])
                        self.possible_duplicates[ids[j]].add(ids[i])
        logging.info(f"Found {len(self.possible_duplicates)} possible "
                     "duplicates for entities.")

    def build_connected_groups(self):
        visited = set()
        groups = []
        for node in self.possible_duplicates:
            if node not in visited:
                queue = deque([node])
                group = set()
                while queue:
                    current = queue.popleft()
                    if current not in visited:
                        visited.add(current)
                        group.add(current)
                        queue.extend(
                            self.possible_duplicates[current] - visited
                        )
                groups.append(group)
        logging.info(f"Identified {len(groups)} connected groups.")
        return groups

    def assign_group_ids(self, groups):
        self.data["dedup_group_id"] = None
        for i, group in enumerate(groups):
            for idx in group:
                self.data.at[idx, "dedup_group_id"] = i
        self.data["dedup_group_id"] = (
            self.data["dedup_group_id"].astype("Int64")
        )

    def fuzzy_grouping(self):
        no_contact = self.data[(self.data["primary_email"].isna()) &
                               (self.data["website_domain"].isna()) &
                               (self.data["company_name"].notna())].copy()
        rows = no_contact.reset_index(drop=True)
        visited = set()
        fuzzy_groups = []

        def score(row1, row2):
            name_score = fuzz.token_sort_ratio(
                self.normalize_name(row1['company_name']),
                self.normalize_name(row2['company_name']))
            descr_score = fuzz.partial_ratio(
                self.clean_text(row1['short_description']),
                self.clean_text(row2['short_description']))
            categ_score = fuzz.partial_ratio(
                self.clean_text(row1['main_business_category']),
                self.clean_text(row2['main_business_category']))
            phone_score = (
                100 if pd.notna(row1['primary_phone']) and
                row1['primary_phone'] == row2['primary_phone'] else 0
            )
            return (0.4 * name_score +
                    0.2 * descr_score +
                    0.2 * categ_score +
                    0.2 * phone_score)

        for i, row1 in rows.iterrows():
            if i in visited:
                continue
            group = {i}
            for j, row2 in rows.iterrows():
                if j in visited or i == j:
                    continue
                if score(row1, row2) >= self.SIMILARITY_THRESHOLD:
                    group.add(j)
            if len(group) > 1:
                fuzzy_groups.append(group)
                visited.update(group)

        for k, group in enumerate(fuzzy_groups, start=20000):
            for idx in group:
                rows.at[idx, 'dedup_group_id'] = k

        no_contact.update(rows[['dedup_group_id']])
        self.data.update(no_contact[['dedup_group_id']])
        logging.info(f"Fuzzy groups identified: {len(fuzzy_groups)}")

    def deduplicate_and_save(self):
        self.data.to_csv("explorare_duplicate_fuzzy.csv", index=False)
        deduped = (
            self.data.sort_values("dedup_group_id",
                                  na_position="last").drop_duplicates(
                "dedup_group_id")
        )
        deduped.to_csv("deduplicat_final.csv", index=False)
        logging.info(f"Saved: {len(deduped)} deduplicated entries.")

    def run(self):
        logging.info('Starting entity resolution....')
        self.find_duplicates()
        groups = self.build_connected_groups()
        self.assign_group_ids(groups)
        self.fuzzy_grouping()
        self.deduplicate_and_save()


if __name__ == '__main__':
    resolver = (
            EntityResolver("veridion_entity_resolution_challenge.snappy.parquet")
        )
    resolver.run()
