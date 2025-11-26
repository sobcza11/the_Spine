from branches.wti_inv_stor_br import build_wti_inv_stor_hist

df_hist = build_wti_inv_stor_hist(start_date="2000-02-07")

print(df_hist.head())
print(df_hist.tail())
print(f"\nRows built: {len(df_hist)}")

