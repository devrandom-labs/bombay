def places:
  sort |
  if length > 12 then .[:12] + ["+" + ((length - 12) | tostring)] else . end |
  join(" ");
