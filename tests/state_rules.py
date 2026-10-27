"""SessionState revision and history rules, without the network."""
import json, sys
from helpers import ROOT
sys.path.insert(0, str(ROOT))
import solution_server as s

st = s.SessionState("L1")
def upd(seg, rev, final, t, text="x"):
    msg = {"type": "CAPTION_UPDATE", "session": "L1", "segment": seg, "revision": rev,
           "source_ts": 1.0, "final": final, "text": text}
    return st.accept(msg, json.dumps(msg), t)

assert upd(1, 1, False, 0.0)
assert upd(1, 1, False, 0.1) is None, "duplicate revision accepted"
assert upd(1, 3, False, 0.2)
assert upd(1, 2, False, 0.3) is None, "older revision accepted"
assert upd(1, 4, True, 0.4)
assert upd(1, 5, False, 0.5) is None, "revision after final accepted"
for seg in range(2, 6):
    assert upd(seg, 1, False, seg) and upd(seg, 2, True, seg + 0.1)
assert [c.segment for c in st.finals_after(0, 0, 60, 6.0)] == [1, 2, 3, 4, 5]
assert [c.segment for c in st.finals_after(3, 2, 60, 6.0)] == [4, 5], "resume cursor ignored"
assert [c.segment for c in st.finals_after(0, 0, 2.5, 6.0)] == [4, 5], "window ignored"
upd(6, 1, False, 6.5)
assert [c.segment for c in st.provisional_after(5)] == [6]
# expiry: everything older than 75 s goes, and expired segments stay rejected
st.prune(80.0)
assert [c.segment for c in st.finals] == [5] and st.floor == 4, (list(st.finals), st.floor)
assert upd(3, 9, True, 80.1) is None, "expired segment re-accepted"
# restart from segment 1 clears the old lecture
assert upd(1, 1, False, 90.0) is None, "stale segment 1 accepted"
st.restart_if_new_lecture(1)
assert upd(1, 1, False, 90.0) and st.max_segment == 1 and not st.finals
# HISTORY JSON is valid and keeps item order
frame = s.history_frames(st.finals_after(0, 0, 60, 0))[0]
assert json.loads(frame) == {"type": "HISTORY", "items": []}
print("state rules: all checks passed")
