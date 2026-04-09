import { ButtonHTMLAttributes } from "react";

type Variant = "default" | "outline" | "ghost";
type Size = "default" | "sm";

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant;
  size?: Size;
}

const variantClasses: Record<Variant, string> = {
  default: "bg-primary text-primary-foreground hover:bg-primary/90",
  outline: "border border-input bg-background hover:bg-accent hover:text-accent-foreground",
  ghost: "hover:bg-accent hover:text-accent-foreground",
};

const sizeClasses: Record<Size, string> = {
  default: "h-10 px-4 py-2",
  sm: "h-9 px-3 text-sm",
};

export function Button({ className = "", variant = "default", size = "default", type = "button", ...props }: ButtonProps) {
  const classes = [
    "inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50",
    variantClasses[variant],
    sizeClasses[size],
    className,
  ].join(" ");

  return <button type={type} className={classes} {...props} />;
}
